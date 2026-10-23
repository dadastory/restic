package resticstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

func TestUploadReopensAndPreservesNativeChunkBoundaries(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	payload := uploadTestPayload(20<<20 + 127)
	var head UploadCheckpoint
	for _, end := range []int{129, 3<<20 + 41, 11<<20 + 97, len(payload)} {
		reopened, err := New(store.config)
		if err != nil {
			t.Fatal(err)
		}
		head, err = reopened.AppendUpload(t.Context(), UploadAppendRequest{
			UploadID: "resume-upload", PreviousSnapshotID: head.SnapshotID, Offset: head.Offset,
			ExpectedSize: int64(len(payload)), CheckpointBytes: 2 << 20,
			Source: bytes.NewReader(payload[head.Offset:end]), Commit: func(ctx context.Context, candidate UploadCheckpoint) error {
				_, verifyErr := store.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{SnapshotID: candidate.SnapshotID, ExpectedSize: candidate.Offset})
				return verifyErr
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if head.Offset != int64(end) {
			t.Fatalf("offset=%d want=%d", head.Offset, end)
		}
		assertUploadBytes(t, reopened, "resume-upload", head, payload[:end])
	}
	continuous, err := store.SnapshotReader(t.Context(), readerBatchRequests([][]byte{payload}, "continuous")[0])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(uploadContentIDs(t, store, head.SnapshotID), uploadContentIDs(t, store, continuous.ID)) {
		t.Fatal("PATCH boundaries changed native content IDs")
	}
	restored := filepath.Join(t.TempDir(), "native-restore")
	if err := store.Restore(t.Context(), RestoreRequest{SnapshotID: head.SnapshotID, DestinationPath: restored}); err != nil {
		t.Fatal(err)
	}
	restoredPayload, err := os.ReadFile(filepath.Join(restored, "payload"))
	if err != nil || !bytes.Equal(restoredPayload, payload) {
		t.Fatalf("standard native restore mismatch: %v", err)
	}
	if err := store.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestUploadCommitsPeriodicPrefixBeforeSourceFailure(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	payload := uploadTestPayload(19<<20 + 51)
	sourceErr := errors.New("interrupted source")
	var committed []UploadCheckpoint
	head, err := store.AppendUpload(t.Context(), UploadAppendRequest{
		UploadID: "failed-source", ExpectedSize: int64(len(payload) + 100), CheckpointBytes: 1 << 20,
		Source: io.MultiReader(bytes.NewReader(payload), uploadErrorReader{sourceErr}),
		Commit: func(_ context.Context, candidate UploadCheckpoint) error {
			committed = append(committed, candidate)
			return nil
		},
	})
	if !errors.Is(err, sourceErr) {
		t.Fatalf("source error=%v", err)
	}
	if len(committed) < 2 || head != committed[len(committed)-1] || head.Offset == 0 {
		t.Fatalf("commits=%v head=%+v", committed, head)
	}
	assertUploadBytes(t, store, "failed-source", head, payload[:head.Offset])
}

func TestUploadRejectedCommitKeepsPreviousHead(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	first, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "cas-upload", ExpectedSize: 11, CheckpointBytes: 1 << 20, Source: bytes.NewBufferString("hello"), Commit: acceptUpload})
	if err != nil {
		t.Fatal(err)
	}
	rejected := errors.New("stale generation")
	var candidate UploadCheckpoint
	result, err := store.AppendUpload(t.Context(), UploadAppendRequest{
		UploadID: "cas-upload", PreviousSnapshotID: first.SnapshotID, Offset: first.Offset, ExpectedSize: 11, CheckpointBytes: 1 << 20, Source: bytes.NewBufferString(" world"),
		Commit: func(_ context.Context, next UploadCheckpoint) error { candidate = next; return rejected },
	})
	if !errors.Is(err, rejected) || result != first {
		t.Fatalf("rejection result=%+v first=%+v error=%v", result, first, err)
	}
	assertUploadBytes(t, store, "cas-upload", first, []byte("hello"))
	assertUploadBytes(t, store, "cas-upload", candidate, []byte("hello world"))
}

func TestUploadFinalizationCleanupPreservesPublishedAndSharedContent(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	payload := uploadTestPayload(3<<20 + 17)
	var heads []UploadCheckpoint
	for _, id := range []string{"published-upload", "shared-upload"} {
		head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: id, ExpectedSize: int64(len(payload)), CheckpointBytes: 1 << 20, Source: bytes.NewReader(payload), Commit: acceptUpload})
		if err != nil {
			t.Fatal(err)
		}
		heads = append(heads, head)
	}
	request := UploadFinalizeRequest{UploadID: "published-upload", SnapshotID: heads[0].SnapshotID, ExpectedSize: int64(len(payload)), IdempotencyKey: "publish-effect"}
	final, err := store.FinalizeUpload(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if final.ID == heads[0].SnapshotID || final.Checksum == "" {
		t.Fatalf("final=%+v", final)
	}
	request.ReconcileExisting = true
	replayed, err := store.FinalizeUpload(t.Context(), request)
	if err != nil || final.ID != replayed.ID || final.Checksum != replayed.Checksum {
		t.Fatalf("replay=%+v error=%v", replayed, err)
	}
	for range 2 {
		if err := store.CleanupUpload(t.Context(), "published-upload"); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := store.OpenUpload(t.Context(), "published-upload", heads[0].SnapshotID); err == nil {
		t.Fatal("checkpoint survived cleanup")
	}
	assertUploadBytes(t, store, "shared-upload", heads[1], payload)
	if err := store.CleanupUpload(t.Context(), "shared-upload"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifySnapshotPayload(t.Context(), VerifySnapshotPayloadRequest{SnapshotID: final.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: final.Checksum}); err != nil {
		t.Fatal(err)
	}
	// Reconciliation remains possible after checkpoint cleanup.
	if replayed, err = store.FinalizeUpload(t.Context(), request); err != nil || replayed.ID != final.ID {
		t.Fatalf("post-cleanup replay=%+v err=%v", replayed, err)
	}
	if err := store.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestUploadRejectsCrossResourceAndInvalidPrefix(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "correct-upload", ExpectedSize: 6, CheckpointBytes: 1 << 20, Source: bytes.NewBufferString("prefix"), Commit: acceptUpload})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.OpenUpload(t.Context(), "wrong-upload", head.SnapshotID); err == nil {
		t.Fatal("opened another upload")
	}
	for _, request := range []UploadAppendRequest{
		{UploadID: "wrong-upload", PreviousSnapshotID: head.SnapshotID, Offset: 6, ExpectedSize: 7},
		{UploadID: "correct-upload", PreviousSnapshotID: head.SnapshotID, Offset: 5, ExpectedSize: 7},
		{UploadID: "correct-upload", PreviousSnapshotID: head.SnapshotID, Offset: 6, ExpectedSize: 5},
	} {
		request.Source, request.Commit, request.CheckpointBytes = bytes.NewBufferString("x"), acceptUpload, 1<<20
		if _, err := store.AppendUpload(t.Context(), request); err == nil {
			t.Fatal("invalid prefix accepted")
		}
	}
	if _, err := store.FinalizeUpload(t.Context(), UploadFinalizeRequest{UploadID: "wrong-upload", SnapshotID: head.SnapshotID, ExpectedSize: 6, IdempotencyKey: "wrong-effect"}); err == nil {
		t.Fatal("finalized another upload")
	}
	if _, err := store.FinalizeUpload(t.Context(), UploadFinalizeRequest{UploadID: "correct-upload", SnapshotID: head.SnapshotID, ExpectedSize: 6, ExpectedChecksum: "sha256:0000000000000000000000000000000000000000000000000000000000000000", IdempotencyKey: "bad-hash"}); err == nil {
		t.Fatal("accepted wrong checksum")
	}
}

func TestUploadZeroLengthCheckpointAndFinalization(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "empty-upload", ExpectedSize: 0, CheckpointBytes: 1 << 20, Source: bytes.NewReader(nil), Commit: acceptUpload})
	if err != nil || head.SnapshotID == "" || head.Offset != 0 {
		t.Fatalf("head=%+v error=%v", head, err)
	}
	assertUploadBytes(t, store, "empty-upload", head, nil)
	if _, err := store.FinalizeUpload(t.Context(), UploadFinalizeRequest{UploadID: "empty-upload", SnapshotID: head.SnapshotID, IdempotencyKey: "empty-effect"}); err != nil {
		t.Fatal(err)
	}
}

func TestUploadConcurrentDistinctAndStaleWriters(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Go(func() {
			id := fmt.Sprintf("parallel-upload-%d", i)
			payload := bytes.Repeat([]byte{byte(i)}, 1<<20)
			head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: id, ExpectedSize: int64(len(payload)), CheckpointBytes: 1 << 20, Source: bytes.NewReader(payload), Commit: acceptUpload})
			if err != nil {
				t.Error(err)
				return
			}
			assertUploadBytes(t, store, id, head, payload)
		})
	}
	wg.Wait()
	var mutex sync.Mutex
	var authoritative UploadCheckpoint
	var succeeded int
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	for i := range 2 {
		wg.Go(func() {
			_, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "raced-upload", ExpectedSize: 1, CheckpointBytes: 1 << 20, Source: bytes.NewReader([]byte{byte(i)}), Commit: func(_ context.Context, candidate UploadCheckpoint) error {
				ready <- struct{}{}
				<-release
				mutex.Lock()
				defer mutex.Unlock()
				if authoritative.SnapshotID != "" {
					return errors.New("stale generation")
				}
				authoritative = candidate
				return nil
			}})
			if err == nil {
				mutex.Lock()
				succeeded++
				mutex.Unlock()
			}
		})
	}
	<-ready
	<-ready
	close(release)
	wg.Wait()
	if succeeded != 1 {
		t.Fatalf("successful writers=%d", succeeded)
	}
	if err := store.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestUploadNativeLocksProtectCheckpointCommitAndOpenReader(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	head, err := store.AppendUpload(t.Context(), UploadAppendRequest{
		UploadID: "locked-upload", ExpectedSize: 7, CheckpointBytes: 1 << 20, Source: bytes.NewBufferString("payload"),
		Commit: func(ctx context.Context, _ UploadCheckpoint) error {
			if err := store.CleanupUpload(ctx, "locked-upload"); err == nil {
				return errors.New("exclusive cleanup ran during checkpoint commit")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.OpenUpload(t.Context(), "locked-upload", head.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CleanupUpload(t.Context(), "locked-upload"); err == nil {
		t.Fatal("cleanup ran while a prefix reader was open")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// Prune unrelated orphans without removing the still-rooted prefix.
	if err := store.CleanupUpload(t.Context(), "unrelated-upload"); err != nil {
		t.Fatal(err)
	}
	assertUploadBytes(t, store, "locked-upload", head, []byte("payload"))
	if err := store.CleanupUpload(t.Context(), "locked-upload"); err != nil {
		t.Fatal(err)
	}
}

func TestUploadCheckpointsAreExcludedFromOrdinaryRetention(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	for i := range 2 {
		if _, err := store.SnapshotReader(t.Context(), readerBatchRequests([][]byte{[]byte("published")}, fmt.Sprintf("retention-%d", i))[0]); err != nil {
			t.Fatal(err)
		}
	}
	head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "retained-upload", ExpectedSize: 7, CheckpointBytes: 1 << 20, Source: bytes.NewBufferString("payload"), Commit: acceptUpload})
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.ForgetPrune(t.Context(), RetentionRequest{KeepLast: 1})
	if err != nil || result.KeptSnapshots != 2 || result.ForgottenSnapshots != 1 {
		t.Fatalf("retention=%+v error=%v", result, err)
	}
	if _, err := store.ForgetPrune(t.Context(), RetentionRequest{ForgetSnapshotIDs: []string{head.SnapshotID}}); err == nil {
		t.Fatal("explicit ordinary retention removed upload root")
	}
	assertUploadBytes(t, store, "retained-upload", head, []byte("payload"))
}

func TestUploadMissingOrCorruptTailFailsWithoutAcknowledgement(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "repository")
			store := initializedReaderStore(t, root)
			payload := uploadTestPayload(64 << 10)
			head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "damaged-upload", ExpectedSize: int64(len(payload) + 1), CheckpointBytes: 1 << 20, Source: bytes.NewReader(payload), Commit: acceptUpload})
			if err != nil {
				t.Fatal(err)
			}
			pack := repositoryDataPack(t, t.Context(), store, root)
			if damage == "missing" {
				if err := os.Remove(pack); err != nil {
					t.Fatal(err)
				}
			} else {
				corruptRepositoryPack(t, pack)
			}
			called := false
			got, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "damaged-upload", PreviousSnapshotID: head.SnapshotID, Offset: head.Offset, ExpectedSize: int64(len(payload) + 1), CheckpointBytes: 1 << 20, Source: bytes.NewBufferString("!"), Commit: func(context.Context, UploadCheckpoint) error { called = true; return nil }})
			if err == nil || called || got.SnapshotID != head.SnapshotID || got.Offset != head.Offset {
				t.Fatalf("damaged prefix append=%+v error=%v called=%v", got, err, called)
			}
			if _, err := store.VerifySnapshotPayload(t.Context(), VerifySnapshotPayloadRequest{SnapshotID: head.SnapshotID, ExpectedSize: int64(len(payload))}); err == nil {
				t.Fatal("native content verification accepted damaged upload")
			}
		})
	}
}

func uploadTestPayload(size int) []byte {
	value := make([]byte, size)
	_, _ = rand.New(rand.NewSource(42)).Read(value)
	return value
}
func acceptUpload(context.Context, UploadCheckpoint) error { return nil }

type uploadErrorReader struct{ err error }

func (r uploadErrorReader) Read([]byte) (int, error) { return 0, r.err }

func assertUploadBytes(t *testing.T, store *Store, id string, head UploadCheckpoint, want []byte) {
	t.Helper()
	reader, size, err := store.OpenUpload(t.Context(), id, head.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || size != int64(len(want)) || !bytes.Equal(got, want) {
		t.Fatalf("prefix mismatch size=%d want=%d read=%v close=%v", size, len(want), err, closeErr)
	}
}

func uploadContentIDs(t *testing.T, store *Store, snapshotID string) []restic.ID {
	t.Helper()
	repo, ctx, unlock, err := store.openLockedRepository(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	defer func() { _ = repo.Close() }()
	if err := repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory); err != nil {
		t.Fatal(err)
	}
	id, err := restic.ParseID(snapshotID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := data.LoadSnapshot(ctx, repo, id)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := data.LoadTree(ctx, repo, *snapshot.Tree)
	if err != nil {
		t.Fatal(err)
	}
	finder := data.NewTreeFinder(tree)
	defer finder.Close()
	node, err := finder.Find("payload")
	if err != nil {
		t.Fatal(err)
	}
	return node.Content
}

func TestUploadDiscardFinalsPreservesCatalogAndOtherUploadRoots(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	payload := uploadTestPayload(2<<20 + 73)
	heads := make(map[string]UploadCheckpoint)
	finals := make(map[string]SnapshotResult)
	for _, id := range []string{"cancelled-effect", "published-effect", "another-owner"} {
		head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: id, ExpectedSize: int64(len(payload)), CheckpointBytes: 1 << 20, Source: bytes.NewReader(payload), Commit: acceptUpload})
		if err != nil {
			t.Fatal(err)
		}
		heads[id] = head
		final, err := store.FinalizeUpload(t.Context(), UploadFinalizeRequest{UploadID: id, SnapshotID: head.SnapshotID, ExpectedSize: int64(len(payload)), IdempotencyKey: "effect:" + id})
		if err != nil {
			t.Fatal(err)
		}
		finals[id] = final
	}
	for range 2 {
		if err := store.DiscardUploadFinals(t.Context(), "cancelled-effect", []string{finals["published-effect"].ID}); err != nil {
			t.Fatal(err)
		}
		if err := store.DiscardUploadFinals(t.Context(), "published-effect", []string{finals["published-effect"].ID}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.VerifySnapshotPayload(t.Context(), VerifySnapshotPayloadRequest{SnapshotID: finals["cancelled-effect"].ID, ExpectedSize: int64(len(payload))}); err == nil {
		t.Fatal("unpublished final root survived cancellation")
	}
	for _, id := range []string{"published-effect", "another-owner"} {
		if _, err := store.VerifySnapshotPayload(t.Context(), VerifySnapshotPayloadRequest{SnapshotID: finals[id].ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: finals[id].Checksum}); err != nil {
			t.Fatal(err)
		}
	}
	assertUploadBytes(t, store, "cancelled-effect", heads["cancelled-effect"], payload)
	if err := store.CleanupUpload(t.Context(), "cancelled-effect"); err != nil {
		t.Fatal(err)
	}
	if err := store.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}

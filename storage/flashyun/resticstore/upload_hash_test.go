package resticstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/restic"
)

func TestUploadHashStateRoundTripAndValidation(t *testing.T) {
	prefix, suffix := uploadTestPayload(129), uploadTestPayload(97)
	digest := sha256.New()
	_, _ = digest.Write(prefix)
	tag, err := uploadHashStateTag(digest, uint64(len(prefix)))
	if err != nil {
		t.Fatal(err)
	}
	if len(tag) > 256 {
		t.Fatalf("hash state is not bounded: %d", len(tag))
	}
	resumed, err := restoreUploadHash(&data.Snapshot{Tags: []string{tag}}, uint64(len(prefix)))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = resumed.Write(suffix)
	want := sha256.Sum256(append(prefix, suffix...))
	if !bytes.Equal(resumed.Sum(nil), want[:]) {
		t.Fatal("resumed standard hash differs")
	}
	for _, snapshot := range []*data.Snapshot{{}, {Tags: []string{tag, tag}}, {Tags: []string{uploadHashStateTagPrefix + "129:broken"}}} {
		if _, err := restoreUploadHash(snapshot, 129); err == nil {
			t.Fatal("accepted missing, duplicate or corrupt hash state")
		}
	}
	if _, err := restoreUploadHash(&data.Snapshot{Tags: []string{tag}}, 130); err == nil {
		t.Fatal("accepted state for a different prefix size")
	}
}

func TestUploadChecksumRequiresExactOwnerAndCompleteSize(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	store := initializedReaderStore(t, root)
	payload := []byte("server-computed checksum")
	head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "checksum-owner", ExpectedSize: int64(len(payload)),
		CheckpointBytes: 1 << 20, Source: bytes.NewReader(payload), Commit: acceptUpload})
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(payload)
	got, err := store.UploadChecksum(t.Context(), "checksum-owner", head.SnapshotID, int64(len(payload)))
	if err != nil || got != "sha256:"+hex.EncodeToString(want[:]) || head.Checksum != got {
		t.Fatalf("checksum=%q err=%v", got, err)
	}
	for _, size := range []int64{-1, int64(len(payload) - 1), int64(len(payload) + 1)} {
		if _, err := store.UploadChecksum(t.Context(), "checksum-owner", head.SnapshotID, size); err == nil {
			t.Fatalf("accepted mismatched size %d", size)
		}
	}
	if _, err := store.UploadChecksum(t.Context(), "other-owner", head.SnapshotID, int64(len(payload))); err == nil {
		t.Fatal("accepted another upload's checkpoint")
	}
	roots, err := os.ReadDir(filepath.Join(root, "snapshots"))
	if err != nil || len(roots) != 1 || roots[0].Name() != head.SnapshotID {
		t.Fatalf("checksum lookup changed snapshot roots: %v %v", roots, err)
	}
}

func TestUploadFinalCheckpointRequiresEOFAndReturnsPrefixChecksum(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	repo, _, unlock, err := store.openLockedRepository(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	factory := repo.ChunkerFactory()
	payload := uploadTestPayload(factory.MaxChunkSize())
	boundary := factory.NewChunker().NextSplitPoint(payload)
	unlock()
	_ = repo.Close()
	if boundary <= 0 {
		t.Fatal("no natural chunk boundary")
	}
	payload = payload[:boundary]
	broken := errors.New("terminal source error")
	for _, test := range []struct {
		name   string
		size   int64
		source io.Reader
		fail   bool
	}{
		{"exact boundary", int64(len(payload)), bytes.NewReader(payload), false},
		{"extra after boundary", int64(len(payload)), io.MultiReader(bytes.NewReader(payload), bytes.NewBufferString("!")), true},
		{"error after boundary", int64(len(payload)), io.MultiReader(bytes.NewReader(payload), uploadErrorReader{broken}), true},
		{"empty EOF", 0, bytes.NewReader(nil), false},
		{"empty error", 0, uploadErrorReader{broken}, true},
		{"empty trailing data", 0, bytes.NewBufferString("!"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			commits := 0
			head, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: test.name, ExpectedSize: test.size,
				CheckpointBytes: 1, Source: test.source, Commit: func(_ context.Context, candidate UploadCheckpoint) error {
					commits++
					want := sha256.Sum256(payload[:candidate.Offset])
					if candidate.Checksum != "sha256:"+hex.EncodeToString(want[:]) {
						t.Fatalf("incorrect candidate prefix checksum: %q", candidate.Checksum)
					}
					return nil
				}})
			if test.fail {
				if err == nil || commits != 0 || head.SnapshotID != "" || head.Offset != 0 {
					t.Fatalf("invalid final source acknowledged: %+v commits=%d err=%v", head, commits, err)
				}
			} else if err != nil || commits != 1 || head.Offset != test.size || head.Checksum == "" {
				t.Fatalf("valid final source not acknowledged: %+v commits=%d err=%v", head, commits, err)
			}
		})
	}
}

func TestUploadHashStateFinalizesWithoutPayloadRescan(t *testing.T) {
	root := filepath.Join(t.TempDir(), "repository")
	store := initializedReaderStore(t, root)
	payload := uploadTestPayload(2<<20 + 137)
	var head UploadCheckpoint
	for _, end := range []int{71, 129, 700001, len(payload)} {
		reopened, err := New(store.config)
		if err != nil {
			t.Fatal(err)
		}
		head, err = reopened.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "hashed-upload", PreviousSnapshotID: head.SnapshotID,
			Offset: head.Offset, ExpectedSize: int64(len(payload)), CheckpointBytes: 1 << 20, Source: bytes.NewReader(payload[head.Offset:end]), Commit: acceptUpload})
		if err != nil {
			t.Fatal(err)
		}
	}
	want := sha256.Sum256(payload)
	checksum := "sha256:" + hex.EncodeToString(want[:])
	// Remove a current data pack only: finalization uses the authenticated upload
	// digest and native tree metadata, while an actual content read still detects it.
	content := uploadContentIDs(t, store, head.SnapshotID)
	repo, err := store.openRepository(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.LoadIndex(t.Context(), restic.NoopTerminalCounterFactory); err != nil {
		_ = repo.Close()
		t.Fatal(err)
	}
	packID := repo.LookupBlob(restic.BlobHandle{Type: restic.DataBlob, ID: content[0]})[0].PackID().String()
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "data", packID[:2], packID)); err != nil {
		t.Fatal(err)
	}
	got, err := store.UploadChecksum(t.Context(), "hashed-upload", head.SnapshotID, int64(len(payload)))
	if err != nil || got != checksum {
		t.Fatalf("metadata-only checksum=%q err=%v", got, err)
	}
	request := UploadFinalizeRequest{UploadID: "hashed-upload", SnapshotID: head.SnapshotID, ExpectedSize: int64(len(payload)),
		ExpectedChecksum: checksum, IdempotencyKey: "hashed-final-effect"}
	final, err := store.FinalizeUpload(t.Context(), request)
	if err != nil || final.Checksum != checksum {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	request.ReconcileExisting = true
	replayed, err := store.FinalizeUpload(t.Context(), request)
	if err != nil || replayed.ID != final.ID || replayed.Checksum != checksum {
		t.Fatalf("replayed=%+v err=%v", replayed, err)
	}
	if _, err := store.VerifySnapshotPayload(t.Context(), VerifySnapshotPayloadRequest{SnapshotID: final.ID, ExpectedSize: int64(len(payload))}); err == nil {
		t.Fatal("native content read did not detect corruption")
	}
}

func TestUploadSupersededRootRetirementPreservesOpenReader(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	first, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "retire-prefix", ExpectedSize: 11, CheckpointBytes: 1 << 20,
		Source: bytes.NewBufferString("hello"), Commit: acceptUpload})
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := store.OpenUpload(t.Context(), "retire-prefix", first.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	second, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "retire-prefix", PreviousSnapshotID: first.SnapshotID, Offset: first.Offset,
		ExpectedSize: 11, CheckpointBytes: 1 << 20, Source: bytes.NewBufferString(" world"), Commit: acceptUpload})
	if err != nil {
		t.Fatal(err)
	}
	if old, _, err := store.OpenUpload(t.Context(), "retire-prefix", first.SnapshotID); err == nil {
		_ = old.Close()
		t.Fatal("superseded checkpoint root was retained")
	}
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != "hello" {
		t.Fatalf("previous reader lost its loaded prefix: %q %v", got, err)
	}
	if err := store.CleanupUpload(t.Context(), "unrelated"); err == nil {
		t.Fatal("prune ignored the active previous-prefix reader")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.CleanupUpload(t.Context(), "unrelated"); err != nil {
		t.Fatal(err)
	}
	assertUploadBytes(t, store, "retire-prefix", second, []byte("hello world"))
}

package resticstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// Bound reads so the regression fails instead of spinning indefinitely on a
// truncated HTTP response, whose body repeatedly returns unexpected EOF.
type boundedHTTPBody struct {
	io.ReadCloser
	reads int
}

func (b *boundedHTTPBody) Read(p []byte) (int, error) {
	b.reads++
	if b.reads > 4 {
		return 0, errors.New("truncated HTTP body was retried without progress")
	}
	return b.ReadCloser.Read(p)
}

func TestStoreReaderRejectsTruncatedHTTPResponseAndAllowsReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "16")
		_, _ = io.WriteString(w, "partial")
	}))
	defer server.Close()
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body := &boundedHTTPBody{ReadCloser: response.Body}
	ctx := t.Context()
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	payload := []byte("complete payload")
	request := readerBatchRequests([][]byte{payload}, "http-replay")[0]
	open := request.Open
	request.Open = func(_ context.Context) (io.ReadCloser, error) { return body, nil }
	snapshot, err := store.SnapshotReader(ctx, request)
	if err == nil || snapshot.ID != "" {
		t.Fatalf("truncated source published snapshot: %#v, %v", snapshot, err)
	}
	if body.reads > 3 {
		t.Fatalf("read truncated HTTP body %d times without progress", body.reads)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.SnapshotCount != 0 {
		t.Fatalf("incomplete source snapshots: %#v, %v", stats, err)
	}
	request.Open = open
	request.ReconcileExisting = true
	snapshot, err = store.SnapshotReader(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{
		SnapshotID: snapshot.ID, ExpectedSize: request.Size, ExpectedChecksum: request.ExpectedChecksum,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStoreCheckIgnoresOrphansButRejectsReferencedDamage(t *testing.T) {
	for _, damage := range []string{"corrupt", "missing", "truncated"} {
		t.Run(damage, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "repository")
			store := initializedReaderStore(t, root)
			ctx := t.Context()
			request := readerBatchRequests([][]byte{bytes.Repeat([]byte("retained payload"), 4096)}, "check-orphan")[0]
			if _, err := store.SnapshotReader(ctx, request); err != nil {
				t.Fatal(err)
			}
			orphan := writeOrphanPack(t, root)
			if err := store.Check(ctx); err != nil {
				t.Fatalf("orphan alone failed integrity: %v", err)
			}
			if _, err := os.Stat(orphan); err != nil {
				t.Fatalf("read-only check changed orphan: %v", err)
			}
			pack := repositoryDataPack(t, ctx, store, root)
			switch damage {
			case "corrupt":
				corruptRepositoryPack(t, pack)
			case "missing":
				if err := os.Remove(pack); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				if err := os.Chmod(pack, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(pack, 1); err != nil {
					t.Fatal(err)
				}
			}
			failure := new(IntegrityError)
			if err := store.Check(ctx); !errors.As(err, &failure) || !failure.CorruptionVerified() {
				t.Fatalf("orphan masked %s data: %v", damage, err)
			}
		})
	}
}

func TestStoreForgetPruneCleansOrphansWithoutNewSnapshotDeletion(t *testing.T) {
	for _, mode := range []string{"retained", "protected", "replayed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			root := filepath.Join(t.TempDir(), "repository")
			store := initializedReaderStore(t, root)
			request := readerBatchRequests([][]byte{[]byte("keep this payload")}, "prune-orphan")[0]
			snapshot, err := store.SnapshotReader(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			retention := RetentionRequest{KeepLast: 1, ProtectedSnapshotIDs: []string{snapshot.ID}}
			if mode != "retained" {
				second := readerBatchRequests([][]byte{[]byte("second payload")}, "second-snapshot")[0]
				other, err := store.SnapshotReader(ctx, second)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "replayed" {
					retention = RetentionRequest{ForgetSnapshotIDs: []string{other.ID}, ProtectedSnapshotIDs: []string{snapshot.ID}}
					if _, err := store.ForgetPrune(ctx, retention); err != nil {
						t.Fatal(err)
					}
				} else {
					retention.ProtectedSnapshotIDs = append(retention.ProtectedSnapshotIDs, other.ID)
				}
			}
			orphan := writeOrphanPack(t, root)
			if _, err := store.ForgetPrune(ctx, retention); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(orphan); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("native prune did not remove orphan: %v", err)
			}
			if _, err := store.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{
				SnapshotID: snapshot.ID, ExpectedSize: request.Size, ExpectedChecksum: request.ExpectedChecksum,
			}); err != nil {
				t.Fatalf("protected payload damaged: %v", err)
			}
			if err := store.Check(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func writeOrphanPack(t *testing.T, root string) string {
	t.Helper()
	// Unindexed bytes are not part of any snapshot and must not be decoded by
	// check. Native prune owns their removal, including incomplete pack uploads.
	payload := []byte("unindexed pack left by an interrupted writer")
	digest := sha256.Sum256(payload)
	id := hex.EncodeToString(digest[:])
	path := filepath.Join(root, "data", id[:2], id)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

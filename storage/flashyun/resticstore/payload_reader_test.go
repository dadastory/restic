package resticstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restic/restic/internal/restic"
)

func snapshotPayloadFixture(t *testing.T, payload []byte) (*Store, string) {
	t.Helper()
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	snapshot, err := store.SnapshotReader(t.Context(), ReaderSnapshotRequest{
		Name: "payload", Size: int64(len(payload)), Hostname: "range-read-test", IdempotencyKey: "range-read-test",
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, snapshot.ID
}

func TestSnapshotPayloadReadsOnlyOverlappingNativeBlobs(t *testing.T) {
	payload := make([]byte, 9<<20)
	_, _ = rand.New(rand.NewSource(12)).Read(payload)
	store, id := snapshotPayloadFixture(t, payload)
	reader, err := store.OpenSnapshotPayload(t.Context(), SnapshotPayloadRequest{SnapshotID: id, ExpectedSize: int64(len(payload))})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var loaded atomic.Int64
	nativeLoad := reader.load
	reader.load = func(ctx context.Context, id restic.ID) ([]byte, error) {
		blob, err := nativeLoad(ctx, id)
		loaded.Add(int64(len(blob)))
		return blob, err
	}
	buf := make([]byte, 2048)
	offset := int64(len(payload) - len(buf))
	n, err := reader.ReadAt(buf, offset)
	if err != nil || n != len(buf) || !bytes.Equal(buf, payload[offset:]) {
		t.Fatalf("end range: n=%d err=%v", n, err)
	}
	first := loaded.Load()
	if first <= 0 || first >= int64(len(payload)) {
		t.Fatalf("loaded %d bytes for an end range of a %d byte payload", first, len(payload))
	}
	_, err = reader.ReadAt(buf, offset)
	if err != nil || loaded.Load() != first {
		t.Fatalf("native blob reuse failed: %v, loaded=%d", err, loaded.Load())
	}
	// Cover random and cross-blob boundaries concurrently through the same reader.
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := range 12 {
				off := int64((worker*997 + i*823117) % (len(payload) - 4096))
				target := make([]byte, 4096)
				n, e := reader.ReadAt(target, off)
				if e != nil || n != len(target) || !bytes.Equal(target, payload[off:off+int64(len(target))]) {
					t.Errorf("range %d: n=%d err=%v", off, n, e)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestSnapshotPayloadBoundsAndRequestCancellation(t *testing.T) {
	store, id := snapshotPayloadFixture(t, []byte("0123456789"))
	if _, err := store.OpenSnapshotPayload(t.Context(), SnapshotPayloadRequest{SnapshotID: id, ExpectedSize: 9}); err == nil {
		t.Fatal("size mismatch accepted")
	}
	if _, err := store.OpenSnapshotPayload(t.Context(), SnapshotPayloadRequest{SnapshotID: "invalid", ExpectedSize: 10}); err == nil {
		t.Fatal("invalid snapshot accepted")
	}
	r, err := store.OpenSnapshotPayload(t.Context(), SnapshotPayloadRequest{SnapshotID: id, ExpectedSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.ReadAt(make([]byte, 1), -1); err == nil {
		t.Fatal("negative offset accepted")
	}
	buf := make([]byte, 5)
	n, err := r.ReadAt(buf, 8)
	if n != 2 || !errors.Is(err, io.EOF) || string(buf[:n]) != "89" {
		t.Fatalf("EOF n=%d err=%v bytes=%q", n, err, buf[:n])
	}
	if n, err := r.ReadAt(nil, 10); n != 0 || err != nil {
		t.Fatalf("empty read n=%d err=%v", n, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.ReadAtContext(ctx, make([]byte, 1), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled range: %v", err)
	}
	// Cancellation of one HTTP request does not revoke the reader's lifetime.
	if _, err := r.ReadAt(make([]byte, 1), 0); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotPayloadCloseCancelsActiveReadAndIsIdempotent(t *testing.T) {
	store, id := snapshotPayloadFixture(t, []byte("payload"))
	r, err := store.OpenSnapshotPayload(t.Context(), SnapshotPayloadRequest{SnapshotID: id, ExpectedSize: 7})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	finished := make(chan error, 1)
	r.load = func(ctx context.Context, _ restic.ID) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	go func() { _, err := r.ReadAt(make([]byte, 1), 0); finished <- err }()
	<-started
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel active read")
	}
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("active read: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAt(make([]byte, 1), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("closed read: %v", err)
	}
}

func TestSnapshotPayloadEmptyFile(t *testing.T) {
	store, id := snapshotPayloadFixture(t, nil)
	r, err := store.OpenSnapshotPayload(t.Context(), SnapshotPayloadRequest{SnapshotID: id, ExpectedSize: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.Size() != 0 {
		t.Fatal("wrong size")
	}
	if n, err := r.ReadAt(make([]byte, 1), 0); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("empty payload: %d %v", n, err)
	}
}

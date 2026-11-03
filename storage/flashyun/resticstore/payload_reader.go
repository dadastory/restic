package resticstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"

	"github.com/restic/restic/internal/bloblru"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

// SnapshotPayloadRequest identifies a catalog-authorized immutable payload.
// Authorization belongs to the caller. A partial read authenticates native
// blobs; it does not re-compute a complete logical-file checksum.
type SnapshotPayloadRequest struct {
	SnapshotID   string
	ExpectedSize int64
}

// SnapshotPayload holds one native repository read lock until Close. It never
// creates a restored plaintext file. Callers can use io.NewSectionReader for
// ordinary HTTP range delivery; ReadAtContext also cancels individual requests.
type SnapshotPayload struct {
	ctx       context.Context
	cancel    context.CancelFunc
	size      int64
	content   restic.IDs
	ends      []int64
	cache     *bloblru.Cache
	load      func(context.Context, restic.ID) ([]byte, error)
	release   func() error
	permit    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func (s *Store) OpenSnapshotPayload(ctx context.Context, request SnapshotPayloadRequest) (*SnapshotPayload, error) {
	id, err := restic.ParseID(request.SnapshotID)
	if err != nil || request.ExpectedSize < 0 {
		return nil, errors.New("invalid snapshot payload request")
	}
	ctx, cancel := context.WithCancel(ctx)
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		cancel()
		return nil, err
	}
	release := func() error { unlock(); return repo.Close() }
	ok := false
	defer func() {
		if !ok {
			cancel()
			_ = release()
		}
	}()
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return nil, s.operationError("load payload index", err)
	}
	snapshot, err := data.LoadSnapshot(lockedCtx, repo, id)
	if err != nil {
		return nil, s.operationError("load payload snapshot", err)
	}
	if snapshot.Tree == nil {
		return nil, errors.New("snapshot payload tree missing")
	}
	tree, err := data.LoadTree(lockedCtx, repo, *snapshot.Tree)
	if err != nil {
		return nil, s.operationError("load payload tree", err)
	}
	finder := data.NewTreeFinder(tree)
	defer finder.Close()
	node, err := finder.Find("payload")
	if err != nil {
		return nil, s.operationError("find payload", err)
	}
	if node == nil || node.Type != data.NodeTypeFile || node.Size > math.MaxInt64 || int64(node.Size) != request.ExpectedSize {
		return nil, errors.New("snapshot payload size or type mismatch")
	}
	reader, err := s.newPayloadReader(lockedCtx, repo, node, request.ExpectedSize, cancel, release)
	if err != nil {
		return nil, err
	}
	ok = true
	return reader, nil
}

// newPayloadReader shares indexed/authenticated blob reads between final
// snapshots and committed upload checkpoints; it does not restore input files.
func (s *Store) newPayloadReader(lockedCtx context.Context, repo *repository.Repository, node *data.Node, expectedSize int64, cancel context.CancelFunc, release func() error) (*SnapshotPayload, error) {
	ends := make([]int64, len(node.Content))
	var total int64
	for i, blob := range node.Content {
		n, found := repo.LookupBlobSize(restic.BlobHandle{Type: restic.DataBlob, ID: blob})
		if !found || uint64(n) > uint64(math.MaxInt64-total) {
			return nil, errors.New("snapshot payload index unavailable")
		}
		total += int64(n)
		ends[i] = total
	}
	if total != expectedSize {
		return nil, errors.New("snapshot payload index size mismatch")
	}
	reader := &SnapshotPayload{ctx: lockedCtx, cancel: cancel, size: total, content: node.Content, ends: ends,
		cache: bloblru.New(16 << 20), permit: make(chan struct{}, 1), release: release,
		load: func(ctx context.Context, blob restic.ID) ([]byte, error) {
			payload, err := repo.LoadBlob(ctx, restic.BlobHandle{Type: restic.DataBlob, ID: blob}, nil)
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, s.operationError("read payload blob", err)
			}
			return payload, nil
		},
	}
	reader.permit <- struct{}{}
	return reader, nil
}

func (r *SnapshotPayload) Size() int64 { return r.size }

func (r *SnapshotPayload) ReadAt(p []byte, off int64) (int, error) {
	return r.ReadAtContext(r.ctx, p, off)
}

func (r *SnapshotPayload) ReadAtContext(ctx context.Context, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative payload offset")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	defer func() { stop(); cancel() }()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.permit:
	}
	defer func() { r.permit <- struct{}{} }()
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	i := sort.Search(len(r.ends), func(i int) bool { return r.ends[i] > off })
	n := 0
	for n < len(p) && i < len(r.content) {
		if err := ctx.Err(); err != nil {
			return n, err
		}
		blob := r.content[i]
		payload, err := r.cache.GetOrCompute(blob, func() ([]byte, error) { return r.load(ctx, blob) })
		if err != nil {
			return n, err
		}
		start := int64(0)
		if i > 0 {
			start = r.ends[i-1]
		}
		if int64(len(payload)) != r.ends[i]-start {
			return n, errors.New("snapshot payload blob size mismatch")
		}
		copied := copy(p[n:], payload[off-start:])
		n += copied
		off += int64(copied)
		i++
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *SnapshotPayload) Close() error {
	r.closeOnce.Do(func() {
		// Cancel before waiting so an active backend read can release the permit.
		r.cancel()
		<-r.permit
		r.closeErr = r.release()
		r.cache = nil
		r.permit <- struct{}{}
	})
	return r.closeErr
}

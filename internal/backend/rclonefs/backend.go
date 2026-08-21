package rclonefs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/object"
	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/backend/layout"
	"github.com/restic/restic/internal/backend/util"
)

var errObjectTooShort = errors.New("repository object is shorter than the requested range")

type ProviderSnapshot struct {
	Backend string
	Root    string
	Options map[string]string
}

type Backend struct {
	filesystem    fs.Fs
	layout        layout.Layout
	capabilities  Capabilities
	connections   uint
	contextCancel context.CancelFunc
	closeOnce     sync.Once
	closeErr      error
	release       func() error
}

var _ backend.Backend = (*Backend)(nil)

func Open(ctx context.Context, snapshot ProviderSnapshot, create bool, connections uint) (*Backend, error) {
	ensureSafeLogging()
	connections, err := normalizeRuntimeConnections(connections)
	if err != nil {
		return nil, err
	}
	capabilities, err := capabilityFor(snapshot)
	if err != nil {
		return nil, err
	}
	registration, err := fs.Find(capabilities.RcloneBackend)
	if err != nil {
		return nil, fmt.Errorf("admitted backend registration is unavailable: %w", err)
	}
	values := make(map[string]string, len(registration.Options)+len(snapshot.Options))
	for _, option := range registration.Options {
		values[option.Name] = fmt.Sprint(option.Default)
	}
	for key, value := range snapshot.Options {
		for _, option := range registration.Options {
			if option.Name == key && option.IsPassword && value != "" {
				value, err = obscure.Obscure(value)
				if err != nil {
					return nil, errors.New("prepare admitted password option")
				}
				break
			}
		}
		values[key] = value
	}
	mapper := newImmutableMapper(values)
	backendContext, finishConstruction, cancelBackend := newBackendConstructionContext(ctx)
	filesystem, err := registration.NewFs(backendContext, "flashyun-"+snapshot.Backend, snapshot.Root, mapper)
	finishConstruction()
	if err != nil && !errors.Is(err, fs.ErrorIsFile) {
		cancelBackend()
		return nil, fmt.Errorf("construct admitted repository backend: %w", err)
	}
	accepted := false
	defer func() {
		if accepted {
			return
		}
		if filesystem != nil && filesystem.Features().Shutdown != nil {
			_ = filesystem.Features().Shutdown(context.Background())
		}
		cancelBackend()
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if mapper.Mutated() {
		return nil, fmt.Errorf("backend attempted unsupported runtime configuration mutation")
	}
	if filesystem == nil {
		return nil, errors.New("admitted repository backend returned no filesystem")
	}
	if capabilities.Publication == publicationTemporaryMove && filesystem.Features().Move == nil {
		return nil, errors.New("backend lacks required atomic publication capability")
	}
	result := &Backend{
		filesystem:    filesystem,
		layout:        layout.NewDefaultLayout("", path.Join),
		capabilities:  capabilities,
		connections:   connections,
		contextCancel: cancelBackend,
	}
	if create {
		if _, statErr := result.Stat(ctx, backend.Handle{Type: backend.ConfigFile}); statErr == nil {
			return nil, errors.New("repository config already exists")
		} else if !result.IsNotExist(statErr) {
			return nil, fmt.Errorf("inspect repository config: %w", statErr)
		}
		if err := filesystem.Mkdir(ctx, ""); err != nil {
			return nil, fmt.Errorf("create repository root: %w", err)
		}
		for _, directory := range result.layout.Paths() {
			if err := filesystem.Mkdir(ctx, strings.TrimSuffix(directory, "/")); err != nil {
				return nil, fmt.Errorf("create repository layout: %w", err)
			}
		}
	}
	accepted = true
	return result, nil
}

// newBackendConstructionContext follows the request only while rclone builds
// the filesystem. Once construction succeeds, it keeps the copied rclone
// configuration alive until the pooled backend is evicted instead of binding
// a reusable client to one HTTP request's cancellation.
func newBackendConstructionContext(ctx context.Context) (context.Context, func(), context.CancelFunc) {
	stableBase := fs.CopyConfig(context.Background(), ctx)
	backendContext, cancelBackend := context.WithCancel(stableBase)
	stopFollowingRequest := context.AfterFunc(ctx, cancelBackend)
	return backendContext, func() { _ = stopFollowingRequest() }, cancelBackend
}

func (storage *Backend) Properties() backend.Properties {
	return backend.Properties{
		Connections:      storage.connections,
		HasAtomicReplace: storage.capabilities.HasAtomicReplace,
	}
}

func (*Backend) Hasher() hash.Hash { return nil }

func (storage *Backend) Save(ctx context.Context, handle backend.Handle, reader backend.RewindReader) error {
	if err := handle.Valid(); err != nil {
		return backoff.Permanent(err)
	}
	remote := storage.layout.Filename(handle)
	if storage.capabilities.Publication == publicationTemporaryMove {
		return storage.saveViaTemporaryMove(ctx, remote, reader)
	}
	return storage.put(ctx, remote, reader)
}

func (storage *Backend) put(ctx context.Context, remote string, reader backend.RewindReader) error {
	source := object.NewStaticObjectInfo(remote, time.Now(), reader.Length(), true, nil, storage.filesystem)
	stored, err := storage.filesystem.Put(ctx, reader, source)
	if err != nil {
		// Admitted direct-publication backends provide atomic replacement. In
		// particular, rclone's S3 Put returns an Object naming the destination
		// even when the multipart upload failed. Removing that value here can
		// delete the previously committed object, which is precisely what
		// Restic's HasAtomicReplace contract forbids after a failed Save.
		return err
	}
	if stored == nil || stored.Size() != reader.Length() {
		if stored != nil {
			_ = stored.Remove(context.WithoutCancel(ctx))
		}
		return errObjectTooShort
	}
	return nil
}

func (storage *Backend) saveViaTemporaryMove(ctx context.Context, remote string, reader backend.RewindReader) (err error) {
	temporaryRemote, err := temporaryName(remote)
	if err != nil {
		return err
	}
	source := object.NewStaticObjectInfo(temporaryRemote, time.Now(), reader.Length(), true, nil, storage.filesystem)
	temporary, err := storage.filesystem.Put(ctx, reader, source)
	if err != nil {
		if temporary != nil {
			_ = temporary.Remove(context.WithoutCancel(ctx))
		}
		return err
	}
	defer func() {
		if err != nil {
			_ = temporary.Remove(context.WithoutCancel(ctx))
		}
	}()
	if temporary.Size() != reader.Length() {
		return errObjectTooShort
	}
	published, err := storage.filesystem.Features().Move(ctx, temporary, remote)
	if err != nil {
		return err
	}
	if published == nil || published.Size() != reader.Length() {
		return errObjectTooShort
	}
	return nil
}

func temporaryName(remote string) (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("create publication identity: %w", err)
	}
	return path.Join(".flashyun-upload", hex.EncodeToString(random), path.Base(remote)), nil
}

func (storage *Backend) Load(ctx context.Context, handle backend.Handle, length int, offset int64, fn func(io.Reader) error) error {
	return util.DefaultLoad(ctx, handle, length, offset, storage.openReader, fn)
}

func (storage *Backend) openReader(ctx context.Context, handle backend.Handle, length int, offset int64) (io.ReadCloser, error) {
	if offset < 0 || length < 0 {
		return nil, backoff.Permanent(errors.New("negative repository range"))
	}
	stored, err := storage.filesystem.NewObject(ctx, storage.layout.Filename(handle))
	if err != nil {
		return nil, err
	}
	if offset > stored.Size() || (length > 0 && offset+int64(length) > stored.Size()) {
		return nil, backoff.Permanent(errObjectTooShort)
	}
	options := []fs.OpenOption{&fs.RangeOption{Start: offset, End: -1}}
	if length > 0 {
		options[0] = &fs.RangeOption{Start: offset, End: offset + int64(length) - 1}
	}
	opened, err := stored.Open(ctx, options...)
	if err != nil {
		return nil, err
	}
	if length > 0 {
		return util.LimitReadCloser(opened, int64(length)), nil
	}
	return opened, nil
}

func (storage *Backend) Stat(ctx context.Context, handle backend.Handle) (backend.FileInfo, error) {
	stored, err := storage.filesystem.NewObject(ctx, storage.layout.Filename(handle))
	if err != nil {
		return backend.FileInfo{}, err
	}
	return backend.FileInfo{Name: handle.Name, Size: stored.Size()}, nil
}

func (storage *Backend) Remove(ctx context.Context, handle backend.Handle) error {
	stored, err := storage.filesystem.NewObject(ctx, storage.layout.Filename(handle))
	if err != nil {
		return err
	}
	return stored.Remove(ctx)
}

func (storage *Backend) List(ctx context.Context, fileType backend.FileType, fn func(backend.FileInfo) error) error {
	if fileType == backend.ConfigFile {
		info, err := storage.Stat(ctx, backend.Handle{Type: backend.ConfigFile})
		if storage.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return fn(info)
	}
	base, recursive := storage.layout.Basedir(fileType)
	return storage.listDirectory(ctx, strings.TrimSuffix(base, "/"), recursive, fn)
}

func (storage *Backend) listDirectory(ctx context.Context, directory string, recursive bool, fn func(backend.FileInfo) error) error {
	entries, err := storage.filesystem.List(ctx, directory)
	if storage.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if stored, ok := entry.(fs.Object); ok {
			if err := fn(backend.FileInfo{Name: path.Base(stored.Remote()), Size: stored.Size()}); err != nil {
				return err
			}
			continue
		}
		if recursive {
			if err := storage.listDirectory(ctx, entry.Remote(), false, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

func (storage *Backend) IsNotExist(err error) bool {
	return errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorDirNotFound)
}

func (storage *Backend) IsPermanentError(err error) bool {
	var permanent *backoff.PermanentError
	return errors.As(err, &permanent) || storage.IsNotExist(err) || errors.Is(err, fs.ErrorPermissionDenied) ||
		errors.Is(err, errObjectTooShort) || fserrors.IsNoRetryError(err)
}

func (storage *Backend) Delete(ctx context.Context) error { return util.DefaultDelete(ctx, storage) }

func (*Backend) Warmup(_ context.Context, _ []backend.Handle) ([]backend.Handle, error) {
	return nil, nil
}
func (*Backend) WarmupWait(_ context.Context, _ []backend.Handle) error { return nil }

func (storage *Backend) Close() error {
	storage.closeOnce.Do(func() {
		if storage.release != nil {
			storage.closeErr = storage.release()
		} else {
			storage.closeErr = storage.shutdown()
		}
	})
	return storage.closeErr
}

func (storage *Backend) shutdown() error {
	defer func() {
		if storage.contextCancel != nil {
			storage.contextCancel()
		}
	}()
	if shutdown := storage.filesystem.Features().Shutdown; shutdown != nil {
		return shutdown(context.Background())
	}
	return nil
}

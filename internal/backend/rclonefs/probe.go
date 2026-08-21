package rclonefs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"time"

	"github.com/rclone/rclone/fs/fserrors"
	"github.com/restic/restic/internal/backend"
)

type ProbeStage struct {
	Code      string
	Duration  time.Duration
	Err       error
	Started   bool
	Retryable bool
}

type ProbeObserver func(ProbeStage) error

type ProbeObject struct {
	Name    string
	Present bool
}

type ProbeObjectObserver func(ProbeObject) error

// ProbeSnapshot exercises the same adapter contract used by Restic against a
// unique repository root. It never invokes an rclone command or config file.
func ProbeSnapshot(ctx context.Context, identity string, snapshot ProviderSnapshot) []ProbeStage {
	return ProbeSnapshotObserved(ctx, identity, snapshot, 30*time.Second, nil, nil)
}

// ProbeSnapshotObserved emits a started and terminal event for each operation
// so the control plane can persist an ordered, live stage state machine.
func ProbeSnapshotObserved(ctx context.Context, identity string, snapshot ProviderSnapshot, stageTimeout time.Duration, observer ProbeObserver, objectObserver ProbeObjectObserver) []ProbeStage {
	stages := make([]ProbeStage, 0, 6)
	var storage *Backend
	run := func(code string, operation func(context.Context) error) bool {
		if observer != nil {
			if err := observer(ProbeStage{Code: code, Started: true}); err != nil {
				stages = append(stages, ProbeStage{Code: code, Err: err})
				return false
			}
		}
		started := time.Now()
		stageCtx := ctx
		cancelStage := func() {}
		if stageTimeout > 0 {
			stageCtx, cancelStage = context.WithTimeout(ctx, stageTimeout)
		}
		err := operation(stageCtx)
		cancelStage()
		stage := ProbeStage{Code: code, Duration: time.Since(started), Err: err, Retryable: retryableProbeError(err)}
		if observer != nil {
			if observerErr := observer(stage); observerErr != nil {
				stage.Err = observerErr
			}
		}
		stages = append(stages, stage)
		return err == nil
	}
	if !run("construct", func(stageCtx context.Context) error {
		var err error
		storage, err = Acquire(stageCtx, identity, snapshot, false, 2)
		return err
	}) {
		return stages
	}
	defer func() { _ = storage.Close() }()

	payload := make([]byte, 64<<10)
	if _, err := rand.Read(payload); err != nil {
		_ = run("write", func(context.Context) error { return err })
		return stages
	}
	payloadDigest := sha256.Sum256(payload)
	handle := backend.Handle{Type: backend.PackFile, Name: hex.EncodeToString(payloadDigest[:])}
	cleanupPending := false
	if !run("write", func(stageCtx context.Context) error {
		if objectObserver != nil {
			if err := objectObserver(ProbeObject{Name: handle.Name, Present: true}); err != nil {
				return err
			}
			cleanupPending = true
		}
		if err := storage.Save(stageCtx, handle, backend.NewByteReader(payload, nil)); err != nil {
			return err
		}
		cleanupPending = true
		return nil
	}) {
		return stages
	}
	defer func() {
		if !cleanupPending {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		err := storage.Remove(cleanupCtx, handle)
		if err == nil || storage.IsNotExist(err) {
			if objectObserver != nil {
				_ = objectObserver(ProbeObject{Name: handle.Name})
			}
		}
	}()
	if !run("range", func(stageCtx context.Context) error {
		return storage.Load(stageCtx, handle, 4096, 1024, func(reader io.Reader) error {
			actual, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			if sha256.Sum256(actual) != sha256.Sum256(payload[1024:5120]) {
				return errObjectTooShort
			}
			return nil
		})
	}) {
		return stages
	}
	if !run("checksum", func(stageCtx context.Context) error {
		var actual []byte
		err := storage.Load(stageCtx, handle, 0, 0, func(reader io.Reader) error {
			var readErr error
			actual, readErr = io.ReadAll(reader)
			return readErr
		})
		if err != nil {
			return err
		}
		if sha256.Sum256(actual) != sha256.Sum256(payload) {
			return errObjectTooShort
		}
		return nil
	}) {
		return stages
	}
	if !run("delete", func(stageCtx context.Context) error {
		if err := storage.Remove(stageCtx, handle); err != nil {
			return err
		}
		if objectObserver != nil {
			if err := objectObserver(ProbeObject{Name: handle.Name}); err != nil {
				return err
			}
		}
		cleanupPending = false
		return nil
	}) {
		return stages
	}
	run("absence", func(stageCtx context.Context) error {
		_, err := storage.Stat(stageCtx, handle)
		if storage.IsNotExist(err) {
			return nil
		}
		return err
	})
	return stages
}

func retryableProbeError(err error) bool {
	return err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || fserrors.IsRetryError(err) || fserrors.ShouldRetry(err))
}

// CleanupProbeObject removes exactly one encrypted probe locator after a
// process restart. The locator is a Restic pack name, never a provider path.
func CleanupProbeObject(ctx context.Context, snapshot ProviderSnapshot, name string) error {
	decoded, err := hex.DecodeString(name)
	if err != nil || len(decoded) != sha256.Size {
		return errors.New("invalid repository probe object")
	}
	storage, err := Open(ctx, snapshot, false, 1)
	if err != nil {
		return err
	}
	defer func() { _ = storage.Close() }()
	handle := backend.Handle{Type: backend.PackFile, Name: name}
	if err := storage.Remove(ctx, handle); err != nil && !storage.IsNotExist(err) {
		return err
	}
	_, err = storage.Stat(ctx, handle)
	if storage.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("repository probe object still exists")
}

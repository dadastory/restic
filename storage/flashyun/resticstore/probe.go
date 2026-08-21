package resticstore

import (
	"context"
	"time"

	"github.com/restic/restic/internal/backend/rclonefs"
)

type ProviderProbeStage struct {
	Code      string
	Duration  time.Duration
	Err       error
	Started   bool
	Retryable bool
}

type ProviderProbeObserver func(ProviderProbeStage) error

type ProviderProbeObject struct {
	Name    string
	Present bool
}

type ProviderProbeObjectObserver func(ProviderProbeObject) error

// ProbeProvider runs the provider-neutral final-repository data-path check.
func ProbeProvider(ctx context.Context, identity string, provider *RepositoryProvider) []ProviderProbeStage {
	return ProbeProviderObserved(ctx, identity, provider, 30*time.Second, nil, nil)
}

// ProbeProviderObserved exposes provider-neutral stage transitions without
// leaking the underlying rclone filesystem or configuration.
func ProbeProviderObserved(ctx context.Context, identity string, provider *RepositoryProvider, stageTimeout time.Duration, observer ProviderProbeObserver, objectObserver ProviderProbeObjectObserver) []ProviderProbeStage {
	if provider == nil {
		return []ProviderProbeStage{{Code: "construct", Err: ErrInvalidProvider}}
	}
	results := rclonefs.ProbeSnapshotObserved(ctx, identity, rclonefs.ProviderSnapshot{
		Backend: provider.Backend, Root: provider.Root, Options: provider.Options,
	}, stageTimeout, func(stage rclonefs.ProbeStage) error {
		if observer == nil {
			return nil
		}
		return observer(ProviderProbeStage{Code: stage.Code, Duration: stage.Duration, Err: stage.Err, Started: stage.Started, Retryable: stage.Retryable})
	}, func(object rclonefs.ProbeObject) error {
		if objectObserver == nil {
			return nil
		}
		return objectObserver(ProviderProbeObject{Name: object.Name, Present: object.Present})
	})
	stages := make([]ProviderProbeStage, 0, len(results))
	for _, result := range results {
		stages = append(stages, ProviderProbeStage{Code: result.Code, Duration: result.Duration, Err: result.Err, Started: result.Started, Retryable: result.Retryable})
	}
	return stages
}

// CleanupProviderProbeObject precisely removes one encrypted probe locator.
func CleanupProviderProbeObject(ctx context.Context, provider *RepositoryProvider, name string) error {
	if provider == nil {
		return ErrInvalidProvider
	}
	return rclonefs.CleanupProbeObject(ctx, rclonefs.ProviderSnapshot{
		Backend: provider.Backend, Root: provider.Root, Options: provider.Options,
	}, name)
}

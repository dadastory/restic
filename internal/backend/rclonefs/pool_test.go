package rclonefs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRuntimePoolReusesOneRevisionAndSeparatesNewRevision(t *testing.T) {
	pool := newRuntimePool(4, 4)
	root := t.TempDir()
	snapshot := ProviderSnapshot{Backend: "local", Root: root}
	first, err := pool.acquire(context.Background(), "provider-a:1", snapshot, true, 2)
	require.NoError(t, err)
	firstFilesystem := first.filesystem
	require.NoError(t, first.Close())

	const workers = 12
	filesystems := make(chan any, workers)
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			lease, acquireErr := pool.acquire(context.Background(), "provider-a:1", snapshot, false, 2)
			if acquireErr != nil {
				errors <- acquireErr
				return
			}
			filesystems <- lease.filesystem
			errors <- lease.Close()
		}()
	}
	wait.Wait()
	close(filesystems)
	close(errors)
	for workerError := range errors {
		require.NoError(t, workerError)
	}
	for filesystem := range filesystems {
		require.Equal(t, firstFilesystem, filesystem)
	}

	secondRevision, err := pool.acquire(context.Background(), "provider-a:2", snapshot, false, 2)
	require.NoError(t, err)
	require.NotEqual(t, firstFilesystem, secondRevision.filesystem)
	require.NoError(t, secondRevision.Close())
	require.NoError(t, pool.close())
}

func TestRuntimePoolSingleFlightsConcurrentConstruction(t *testing.T) {
	pool := newRuntimePool(4, 4)
	snapshot := ProviderSnapshot{Backend: "local", Root: t.TempDir()}
	const workers = 12
	start := make(chan struct{})
	filesystems := make(chan any, workers)
	errors := make(chan error, workers)
	leases := make(chan *Backend, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			lease, err := pool.acquire(context.Background(), "provider-a:1", snapshot, true, 2)
			if err != nil {
				errors <- err
				return
			}
			filesystems <- lease.filesystem
			leases <- lease
		}()
	}
	close(start)
	wait.Wait()
	close(filesystems)
	close(errors)
	close(leases)
	for err := range errors {
		require.NoError(t, err)
	}
	var shared any
	for filesystem := range filesystems {
		if shared == nil {
			shared = filesystem
		}
		require.Equal(t, shared, filesystem)
	}
	for lease := range leases {
		require.NoError(t, lease.Close())
	}
	require.Len(t, pool.entries, 1)
	require.NoError(t, pool.close())
}

func TestRuntimePoolEvictsOldestIdleRevisionAtBound(t *testing.T) {
	pool := newRuntimePool(2, 4)
	keys := make(map[string]string)
	for _, identity := range []string{"provider-a:1", "provider-b:1", "provider-c:1"} {
		snapshot := ProviderSnapshot{Backend: "local", Root: t.TempDir()}
		key, _, err := canonicalRuntimeKey(identity, snapshot, 2)
		require.NoError(t, err)
		keys[identity] = key
		lease, err := pool.acquire(context.Background(), identity, snapshot, true, 2)
		require.NoError(t, err)
		require.NoError(t, lease.Close())
	}
	require.Len(t, pool.entries, 2)
	require.NotContains(t, pool.entries, keys["provider-a:1"])
	require.Contains(t, pool.entries, keys["provider-b:1"])
	require.Contains(t, pool.entries, keys["provider-c:1"])
	require.NoError(t, pool.close())
}

func TestRuntimePoolNormalizesDefaultConnectionsBeforeLookup(t *testing.T) {
	pool := newRuntimePool(4, 4)
	snapshot := ProviderSnapshot{Backend: "local", Root: t.TempDir()}
	implicit, err := pool.acquire(context.Background(), "provider-a:1", snapshot, true, 0)
	require.NoError(t, err)
	filesystem := implicit.filesystem
	require.Equal(t, uint(defaultRuntimeConnections), implicit.connections)
	require.NoError(t, implicit.Close())

	explicit, err := pool.acquire(context.Background(), "provider-a:1", snapshot, false, defaultRuntimeConnections)
	require.NoError(t, err)
	require.Equal(t, filesystem, explicit.filesystem)
	require.NoError(t, explicit.Close())
	require.Len(t, pool.entries, 1)

	reduced, err := pool.acquire(context.Background(), "provider-a:1", snapshot, false, 2)
	require.NoError(t, err)
	require.NotEqual(t, filesystem, reduced.filesystem)
	require.NoError(t, reduced.Close())
	require.Len(t, pool.entries, 2)
	require.NoError(t, pool.close())
}

func TestRuntimePoolBindsExternalIdentityToProviderSnapshotWithoutExposingValues(t *testing.T) {
	pool := newRuntimePool(4, 4)
	firstSnapshot := ProviderSnapshot{Backend: "local", Root: t.TempDir(), Options: map[string]string{"secret": "first-secret-value"}}
	secondSnapshot := ProviderSnapshot{Backend: "local", Root: t.TempDir(), Options: map[string]string{"secret": "second-secret-value"}}
	firstKey, _, err := canonicalRuntimeKey("provider-a:1", firstSnapshot, 0)
	require.NoError(t, err)
	secondKey, _, err := canonicalRuntimeKey("provider-a:1", secondSnapshot, defaultRuntimeConnections)
	require.NoError(t, err)
	require.NotEqual(t, firstKey, secondKey)
	orderedKey, _, err := canonicalRuntimeKey("provider-a:1", ProviderSnapshot{
		Backend: "local", Root: firstSnapshot.Root, Options: map[string]string{"alpha": "one", "omega": "two"},
	}, 0)
	require.NoError(t, err)
	reversedKey, _, err := canonicalRuntimeKey("provider-a:1", ProviderSnapshot{
		Backend: "local", Root: firstSnapshot.Root, Options: map[string]string{"omega": "two", "alpha": "one"},
	}, defaultRuntimeConnections)
	require.NoError(t, err)
	require.Equal(t, orderedKey, reversedKey)
	for _, key := range []string{firstKey, secondKey} {
		require.NotContains(t, key, firstSnapshot.Root)
		require.NotContains(t, key, secondSnapshot.Root)
		require.NotContains(t, key, "secret")
		require.NotContains(t, key, "first-secret-value")
		require.NotContains(t, key, "second-secret-value")
	}
	_, _, err = canonicalRuntimeKey("provider-a:1", firstSnapshot, maximumRuntimeConnections+1)
	require.Error(t, err)
	require.NotContains(t, err.Error(), firstSnapshot.Root)
	require.NotContains(t, err.Error(), "first-secret-value")

	firstSnapshot.Options = nil
	secondSnapshot.Options = nil
	first, err := pool.acquire(context.Background(), "provider-a:1", firstSnapshot, true, 0)
	require.NoError(t, err)
	firstFilesystem := first.filesystem
	require.NoError(t, first.Close())
	second, err := pool.acquire(context.Background(), "provider-a:1", secondSnapshot, true, 0)
	require.NoError(t, err)
	require.NotEqual(t, firstFilesystem, second.filesystem)
	require.NoError(t, second.Close())
	require.Len(t, pool.entries, 2)
	require.NoError(t, pool.close())
}

func TestRuntimePoolBoundsDistinctColdConstructionWithoutBlockingWarmReuse(t *testing.T) {
	pool := newRuntimePool(8, 2)
	warmSnapshot := ProviderSnapshot{Backend: "local", Root: t.TempDir()}
	warm, err := pool.acquire(context.Background(), "warm:1", warmSnapshot, true, 0)
	require.NoError(t, err)
	warmFilesystem := warm.filesystem
	require.NoError(t, warm.Close())

	originalOpen := pool.open
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	var active atomic.Int64
	var maximum atomic.Int64
	pool.open = func(ctx context.Context, snapshot ProviderSnapshot, create bool, connections uint) (*Backend, error) {
		current := active.Add(1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			active.Add(-1)
			return nil, ctx.Err()
		case <-release:
		}
		active.Add(-1)
		return originalOpen(ctx, snapshot, create, connections)
	}

	type result struct {
		backend *Backend
		err     error
	}
	results := make(chan result, 3)
	for index := range 3 {
		go func() {
			backend, acquireErr := pool.acquire(context.Background(), "cold:"+string(rune('a'+index)), ProviderSnapshot{
				Backend: "local", Root: t.TempDir(),
			}, true, 0)
			results <- result{backend: backend, err: acquireErr}
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("cold construction did not reach the configured admission limit")
		}
	}
	select {
	case <-entered:
		t.Fatal("a third cold constructor bypassed admission")
	case <-time.After(25 * time.Millisecond):
	}

	warmAgain, err := pool.acquire(context.Background(), "warm:1", warmSnapshot, false, defaultRuntimeConnections)
	require.NoError(t, err)
	require.Equal(t, warmFilesystem, warmAgain.filesystem)
	require.NoError(t, warmAgain.Close())

	release <- struct{}{}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("waiting cold construction did not resume")
	}
	release <- struct{}{}
	release <- struct{}{}
	for range 3 {
		completed := <-results
		require.NoError(t, completed.err)
		require.NoError(t, completed.backend.Close())
	}
	require.Equal(t, int64(2), maximum.Load())
	require.NoError(t, pool.close())
}

func TestRuntimePoolCancelsConstructionAdmissionAndAllowsRetry(t *testing.T) {
	pool := newRuntimePool(4, 1)
	originalOpen := pool.open
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	pool.open = func(ctx context.Context, snapshot ProviderSnapshot, create bool, connections uint) (*Backend, error) {
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return originalOpen(ctx, snapshot, create, connections)
		}
	}
	firstDone := make(chan error, 1)
	go func() {
		backend, err := pool.acquire(context.Background(), "first:1", ProviderSnapshot{Backend: "local", Root: t.TempDir()}, true, 0)
		if err == nil {
			err = backend.Close()
		}
		firstDone <- err
	}()
	<-entered

	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()
	secondSnapshot := ProviderSnapshot{Backend: "local", Root: t.TempDir()}
	secondKey, _, err := canonicalRuntimeKey("second:1", secondSnapshot, 0)
	require.NoError(t, err)
	_, err = pool.acquire(cancelledContext, "second:1", secondSnapshot, true, 0)
	require.ErrorIs(t, err, context.Canceled)
	pool.mu.Lock()
	require.NotContains(t, pool.constructing, secondKey)
	pool.mu.Unlock()

	release <- struct{}{}
	require.NoError(t, <-firstDone)
	pool.open = originalOpen
	retry, err := pool.acquire(context.Background(), "second:1", secondSnapshot, true, 0)
	require.NoError(t, err)
	require.NoError(t, retry.Close())
	require.NoError(t, pool.close())
}

func TestNormalizeRuntimeConnectionsRejectsOutOfRange(t *testing.T) {
	connections, err := normalizeRuntimeConnections(0)
	require.NoError(t, err)
	require.Equal(t, uint(defaultRuntimeConnections), connections)
	connections, err = normalizeRuntimeConnections(defaultRuntimeConnections)
	require.NoError(t, err)
	require.Equal(t, uint(defaultRuntimeConnections), connections)
	_, err = normalizeRuntimeConnections(maximumRuntimeConnections + 1)
	require.True(t, errors.Is(err, errInvalidRuntimeConnections))
}

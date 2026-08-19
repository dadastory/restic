//go:build integration

package flashyun

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

const (
	sameFileWorkers        = 32
	independentFileWorkers = 256
)

type integrationScopedAuthorizer struct{}

func (integrationScopedAuthorizer) Authorize(_ context.Context, subject, object, action string) (bool, error) {
	resource, err := ParseObject(object)
	if err != nil {
		return false, err
	}
	return subject == resource.Workspace && action == "write", nil
}

func TestRedisFileLeaseHighConcurrency(t *testing.T) {
	redisURL := os.Getenv("FLASHYUN_STORAGE_TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("FLASHYUN_STORAGE_TEST_REDIS_URL is not configured; real Redis concurrency was not verified")
	}

	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("ParseURL(%q) error = %v", redisURL, err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Redis Ping() error = %v", err)
	}

	locker, err := NewRedisLeaseLocker(client, RedisLeaseConfig{
		TTL:           5 * time.Second,
		RetryStrategy: redislock.LinearBackoff(2 * time.Millisecond),
	})
	if err != nil {
		t.Fatalf("NewRedisLeaseLocker() error = %v", err)
	}
	coordinator, err := NewCoordinator(integrationScopedAuthorizer{}, locker, CoordinatorConfig{
		RenewInterval: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("stress-%d", time.Now().UnixNano())
	sameResource := Resource{Tenant: prefix, Workspace: "workspace-a", File: "same-file"}

	var sameActive, sameMax, allActive, allMax atomic.Int32
	start := make(chan struct{})
	errs := make(chan error, sameFileWorkers+independentFileWorkers)
	var workers sync.WaitGroup

	run := func(subject string, resource Resource, sameFile bool) {
		defer workers.Done()
		<-start
		err := coordinator.Mutate(ctx, Request{Subject: subject, Resource: resource, Action: "write"}, func(context.Context) error {
			active := allActive.Add(1)
			updateMax(&allMax, active)
			defer allActive.Add(-1)

			if sameFile {
				current := sameActive.Add(1)
				updateMax(&sameMax, current)
				defer sameActive.Add(-1)
			}

			time.Sleep(25 * time.Millisecond)
			return nil
		})
		if err != nil {
			errs <- err
		}
	}

	for i := 0; i < sameFileWorkers; i++ {
		workers.Add(1)
		go run(sameResource.Workspace, sameResource, true)
	}
	for i := 0; i < independentFileWorkers; i++ {
		workspace := fmt.Sprintf("workspace-%02d", i%16)
		workers.Add(1)
		go run(workspace, Resource{Tenant: prefix, Workspace: workspace, File: fmt.Sprintf("file-%03d", i)}, false)
	}

	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent mutation error = %v", err)
	}

	if got := sameMax.Load(); got != 1 {
		t.Fatalf("same-file max active mutations = %d, want 1", got)
	}
	if got := allMax.Load(); got < 2 {
		t.Fatalf("independent files never overlapped: max active mutations = %d", got)
	}
	var crossUserMutationRan atomic.Bool
	err = coordinator.Mutate(ctx, Request{Subject: "workspace-foreign", Resource: Resource{Tenant: prefix, Workspace: "workspace-00", File: "private-file"}, Action: "write"}, func(context.Context) error {
		crossUserMutationRan.Store(true)
		return nil
	})
	if err != ErrAccessDenied {
		t.Fatalf("cross-user mutation error = %v, want %v", err, ErrAccessDenied)
	}
	if crossUserMutationRan.Load() {
		t.Fatal("cross-user mutation ran after authorization denial")
	}
}

func updateMax(target *atomic.Int32, candidate int32) {
	for current := target.Load(); candidate > current; current = target.Load() {
		if target.CompareAndSwap(current, candidate) {
			return
		}
	}
}

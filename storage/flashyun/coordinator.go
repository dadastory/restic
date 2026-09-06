package flashyun

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrAccessDenied is returned before a lease is acquired when the
	// authorization boundary denies a requested storage mutation.
	ErrAccessDenied = errors.New("flashyun storage access denied")
	// ErrLeaseLost is returned when a held distributed file lease cannot be
	// refreshed while protected work is running.
	ErrLeaseLost = errors.New("flashyun storage file lease lost")
)

// Lease is a token-owned, renewable lock for one logical file resource.
type Lease interface {
	Refresh(ctx context.Context) error
	Release(ctx context.Context) error
}

// LeaseLocker obtains a lease for exactly one logical file resource.
type LeaseLocker interface {
	// LeaseTTL returns the effective lifetime used when this locker acquires or
	// refreshes a lease. The coordinator uses it to keep renewal safely ahead
	// of expiry.
	LeaseTTL() time.Duration
	Acquire(ctx context.Context, resource Resource) (Lease, error)
}

// Request describes one protected logical file mutation.
type Request struct {
	Subject  string
	Resource Resource
	Action   string
}

// CoordinatorConfig controls lease renewal and cleanup.
type CoordinatorConfig struct {
	RenewInterval  time.Duration
	CleanupTimeout time.Duration
}

// Coordinator authorizes and serializes a mutable operation for one logical
// file. It deliberately has no global mutation lock.
type Coordinator struct {
	authorizer     Authorizer
	locker         LeaseLocker
	renewInterval  time.Duration
	cleanupTimeout time.Duration
}

// NewCoordinator constructs a file-scoped storage coordinator.
func NewCoordinator(authorizer Authorizer, locker LeaseLocker, config CoordinatorConfig) (*Coordinator, error) {
	if authorizer == nil {
		return nil, errors.New("flashyun storage authorizer is required")
	}
	if locker == nil {
		return nil, errors.New("flashyun storage lease locker is required")
	}
	if config.RenewInterval < 0 {
		return nil, errors.New("flashyun storage renew interval cannot be negative")
	}
	if config.CleanupTimeout < 0 {
		return nil, errors.New("flashyun storage cleanup timeout cannot be negative")
	}
	leaseTTL := locker.LeaseTTL()
	if leaseTTL <= time.Nanosecond {
		return nil, errors.New("flashyun storage lease TTL must exceed one nanosecond")
	}
	if config.RenewInterval == 0 {
		config.RenewInterval = leaseTTL / 3
		if config.RenewInterval == 0 {
			config.RenewInterval = time.Nanosecond
		}
	}
	if config.RenewInterval >= leaseTTL {
		return nil, errors.New("flashyun storage renew interval must be shorter than lease TTL")
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = time.Second
	}

	return &Coordinator{
		authorizer:     authorizer,
		locker:         locker,
		renewInterval:  config.RenewInterval,
		cleanupTimeout: config.CleanupTimeout,
	}, nil
}

// Mutate authorizes the request, acquires a lease for only the request's file,
// and executes mutation. The mutation must honor its context so lease loss can
// stop unsafe work promptly.
func (c *Coordinator) Mutate(ctx context.Context, request Request, mutation func(context.Context) error) error {
	if request.Subject == "" || request.Action == "" {
		return errors.New("flashyun storage subject and action are required")
	}
	if mutation == nil {
		return errors.New("flashyun storage mutation is required")
	}
	object, err := request.Resource.Object()
	if err != nil {
		return err
	}

	allowed, err := c.authorizer.Authorize(ctx, request.Subject, object, request.Action)
	if err != nil {
		return fmt.Errorf("authorize storage mutation: %w", err)
	}
	if !allowed {
		return ErrAccessDenied
	}

	lease, err := c.locker.Acquire(ctx, request.Resource)
	if err != nil {
		return fmt.Errorf("acquire file lease: %w", err)
	}

	workCtx, cancelWork := context.WithCancel(ctx)
	renewalDone := make(chan error, 1)
	go c.renewLease(workCtx, cancelWork, lease, renewalDone)

	mutationErr := mutation(workCtx)
	cancelWork()
	renewalErr := <-renewalDone

	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), c.cleanupTimeout)
	releaseErr := lease.Release(cleanupCtx)
	cancelCleanup()

	if renewalErr != nil {
		mutationErr = errors.Join(mutationErr, ErrLeaseLost, renewalErr)
	}
	if releaseErr != nil {
		mutationErr = errors.Join(mutationErr, fmt.Errorf("release file lease: %w", releaseErr))
	}
	return mutationErr
}

func (c *Coordinator) renewLease(ctx context.Context, cancelWork context.CancelFunc, lease Lease, done chan<- error) {
	ticker := time.NewTicker(c.renewInterval)
	defer ticker.Stop()
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			if err := lease.Refresh(ctx); err != nil {
				cancelWork()
				done <- err
				return
			}
		}
	}
}

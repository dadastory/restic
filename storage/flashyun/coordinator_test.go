package flashyun

import (
	"context"
	"errors"
	"testing"
	"time"
)

type testAuthorizer struct {
	allowed bool
	err     error
	calls   int
}

func (a *testAuthorizer) Authorize(_ context.Context, _, _, _ string) (bool, error) {
	a.calls++
	return a.allowed, a.err
}

type testLeaseLocker struct {
	acquireCalls int
	ttl          time.Duration
}

func (l *testLeaseLocker) Acquire(_ context.Context, _ Resource) (Lease, error) {
	l.acquireCalls++
	return &testLease{}, nil
}

func (l *testLeaseLocker) LeaseTTL() time.Duration {
	if l.ttl == 0 {
		return time.Second
	}
	return l.ttl
}

type testLease struct{}

func (*testLease) Refresh(context.Context) error { return nil }
func (*testLease) Release(context.Context) error { return nil }

type trackingLease struct {
	refreshErr error
	released   bool
	releaseErr error
}

func (l *trackingLease) Refresh(context.Context) error { return l.refreshErr }

func (l *trackingLease) Release(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	l.released = true
	return l.releaseErr
}

type fixedLeaseLocker struct {
	lease Lease
	ttl   time.Duration
}

func (l fixedLeaseLocker) Acquire(context.Context, Resource) (Lease, error) {
	return l.lease, nil
}

func (l fixedLeaseLocker) LeaseTTL() time.Duration {
	if l.ttl == 0 {
		return time.Second
	}
	return l.ttl
}

func TestCoordinatorDerivesRenewalIntervalBeforeLeaseTTL(t *testing.T) {
	t.Parallel()

	const ttl = 90 * time.Millisecond
	coordinator, err := NewCoordinator(&testAuthorizer{allowed: true}, fixedLeaseLocker{
		lease: &testLease{},
		ttl:   ttl,
	}, CoordinatorConfig{})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}
	if coordinator.renewInterval <= 0 {
		t.Fatalf("renew interval = %s, want positive", coordinator.renewInterval)
	}
	if coordinator.renewInterval >= ttl {
		t.Fatalf("renew interval = %s, want less than TTL %s", coordinator.renewInterval, ttl)
	}
}

func TestCoordinatorRejectsRenewalAtOrAfterLeaseTTL(t *testing.T) {
	t.Parallel()

	const ttl = time.Second
	_, err := NewCoordinator(&testAuthorizer{allowed: true}, fixedLeaseLocker{
		lease: &testLease{},
		ttl:   ttl,
	}, CoordinatorConfig{RenewInterval: ttl})
	if err == nil {
		t.Fatal("NewCoordinator() error = nil, want renewal interval validation error")
	}
}

func TestCoordinatorDeniesBeforeLeaseAndMutation(t *testing.T) {
	t.Parallel()

	authorizer := &testAuthorizer{}
	locker := &testLeaseLocker{}
	coordinator, err := NewCoordinator(authorizer, locker, CoordinatorConfig{})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}

	mutated := false
	err = coordinator.Mutate(context.Background(), Request{
		Subject:  "alice",
		Resource: Resource{Tenant: "tenant-a", Workspace: "workspace-a", File: "file-a"},
		Action:   "write",
	}, func(context.Context) error {
		mutated = true
		return nil
	})

	if !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("Mutate() error = %v, want ErrAccessDenied", err)
	}
	if authorizer.calls != 1 {
		t.Fatalf("authorizer calls = %d, want 1", authorizer.calls)
	}
	if locker.acquireCalls != 0 {
		t.Fatalf("lease acquisitions = %d, want 0", locker.acquireCalls)
	}
	if mutated {
		t.Fatal("mutation ran despite denied authorization")
	}
}

func TestCoordinatorPermitsAuthorizedMutation(t *testing.T) {
	t.Parallel()

	authorizer := &testAuthorizer{allowed: true}
	locker := &testLeaseLocker{}
	coordinator, err := NewCoordinator(authorizer, locker, CoordinatorConfig{})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}

	mutated := false
	err = coordinator.Mutate(context.Background(), Request{
		Subject:  "alice",
		Resource: Resource{Tenant: "tenant-a", Workspace: "workspace-a", File: "file-a"},
		Action:   "write",
	}, func(context.Context) error {
		mutated = true
		return nil
	})
	if err != nil {
		t.Fatalf("Mutate() error = %v", err)
	}
	if locker.acquireCalls != 1 {
		t.Fatalf("lease acquisitions = %d, want 1", locker.acquireCalls)
	}
	if !mutated {
		t.Fatal("mutation did not run after authorization and lease acquisition")
	}
}

func TestAuthorizeReceivesStableResourceObject(t *testing.T) {
	t.Parallel()

	resource := Resource{Tenant: "tenant-a", Workspace: "workspace-a", File: "file-a"}
	object, err := resource.Object()
	if err != nil {
		t.Fatalf("Object() error = %v", err)
	}

	var receivedSubject, receivedObject, receivedAction string
	authorizer := AuthorizerFunc(func(_ context.Context, subject, object, action string) (bool, error) {
		receivedSubject, receivedObject, receivedAction = subject, object, action
		return action == "write", nil
	})

	allowed, err := authorizer.Authorize(context.Background(), "alice", object, "write")
	if err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}
	if !allowed {
		t.Fatal("Authorize() denied the configured policy")
	}
	if receivedSubject != "alice" || receivedObject != object || receivedAction != "write" {
		t.Fatalf("Authorize() received subject=%q object=%q action=%q, want the stable resource object", receivedSubject, receivedObject, receivedAction)
	}

	denied, err := authorizer.Authorize(context.Background(), "alice", object, "delete")
	if err != nil {
		t.Fatalf("Authorize(delete) error = %v", err)
	}
	if denied {
		t.Fatal("Authorize(delete) permitted a missing policy")
	}
}

func TestCoordinatorCancelsWorkWhenLeaseRenewalFails(t *testing.T) {
	lease := &trackingLease{refreshErr: errors.New("Redis unavailable")}
	coordinator, err := NewCoordinator(&testAuthorizer{allowed: true}, fixedLeaseLocker{lease: lease}, CoordinatorConfig{
		RenewInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}

	err = coordinator.Mutate(context.Background(), Request{
		Subject:  "alice",
		Resource: Resource{Tenant: "tenant-a", Workspace: "workspace-a", File: "file-a"},
		Action:   "write",
	}, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Mutate() error = %v, want ErrLeaseLost", err)
	}
	if !lease.released {
		t.Fatal("lease was not released after renewal failure")
	}
}

func TestCoordinatorReleasesLeaseAfterCallerCancellation(t *testing.T) {
	lease := &trackingLease{}
	coordinator, err := NewCoordinator(&testAuthorizer{allowed: true}, fixedLeaseLocker{lease: lease}, CoordinatorConfig{})
	if err != nil {
		t.Fatalf("NewCoordinator() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = coordinator.Mutate(ctx, Request{
		Subject:  "alice",
		Resource: Resource{Tenant: "tenant-a", Workspace: "workspace-a", File: "file-a"},
		Action:   "write",
	}, func(ctx context.Context) error {
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Mutate() error = %v, want context.Canceled", err)
	}
	if !lease.released {
		t.Fatal("lease release inherited cancellation from caller context")
	}
}

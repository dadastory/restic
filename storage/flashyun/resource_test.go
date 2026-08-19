package flashyun

import (
	"testing"
)

func TestResourceLockKeyRequiresEveryScope(t *testing.T) {
	t.Parallel()

	for _, resource := range []Resource{
		{Workspace: "workspace-a", File: "file-a"},
		{Tenant: "tenant-a", File: "file-a"},
		{Tenant: "tenant-a", Workspace: "workspace-a"},
	} {
		if _, err := resource.LockKey(); err == nil {
			t.Fatalf("LockKey(%+v) succeeded for incomplete scope", resource)
		}
	}
}

func TestResourceLockKeyIsDeterministicAndIsolated(t *testing.T) {
	t.Parallel()

	base := Resource{Tenant: "tenant-a", Workspace: "workspace-a", File: "file-a"}
	baseKey, err := base.LockKey()
	if err != nil {
		t.Fatalf("base LockKey() error = %v", err)
	}

	repeatedKey, err := base.LockKey()
	if err != nil {
		t.Fatalf("repeated LockKey() error = %v", err)
	}
	if repeatedKey != baseKey {
		t.Fatalf("same resource key changed: first %q, second %q", baseKey, repeatedKey)
	}

	for _, independent := range []Resource{
		{Tenant: "tenant-b", Workspace: "workspace-a", File: "file-a"},
		{Tenant: "tenant-a", Workspace: "workspace-b", File: "file-a"},
		{Tenant: "tenant-a", Workspace: "workspace-a", File: "file-b"},
		{Tenant: "tenant-a:workspace-a", Workspace: "file-a", File: "file-b"},
	} {
		key, err := independent.LockKey()
		if err != nil {
			t.Fatalf("independent LockKey(%+v) error = %v", independent, err)
		}
		if key == baseKey {
			t.Fatalf("independent resource %+v collided with %+v", independent, base)
		}
	}
}

func TestResourceObjectRoundTripsForApplicationAuthorizationAdapters(t *testing.T) {
	t.Parallel()
	resource := Resource{Tenant: "tenant:一", Workspace: "workspace/二", File: "file:三"}
	object, err := resource.Object()
	if err != nil {
		t.Fatalf("Object() error = %v", err)
	}
	parsed, err := ParseObject(object)
	if err != nil {
		t.Fatalf("ParseObject() error = %v", err)
	}
	if parsed != resource {
		t.Fatalf("ParseObject() = %#v, want %#v", parsed, resource)
	}
	if _, err := ParseObject("flashyun:file:not-base64:workspace:file"); err == nil {
		t.Fatal("ParseObject(invalid) error = nil")
	}
}

package flashyun

import (
	"encoding/base64"
	"errors"
	"strings"
)

var errInvalidResource = errors.New("flashyun storage resource requires tenant, workspace, and file")

// Resource identifies a logical FlashYun file. A resource key is deliberately
// narrower than a Restic repository: different files can be mutated in parallel.
type Resource struct {
	Tenant    string
	Workspace string
	File      string
}

// Validate checks that a resource contains every isolation scope.
func (r Resource) Validate() error {
	if r.Tenant == "" || r.Workspace == "" || r.File == "" {
		return errInvalidResource
	}
	return nil
}

// Object returns the stable policy object used to authorize this resource.
func (r Resource) Object() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	return "flashyun:file:" + encodeScope(r.Tenant) + ":" + encodeScope(r.Workspace) + ":" + encodeScope(r.File), nil
}

// ParseObject decodes the stable policy object returned by Resource.Object.
// Application adapters use it to map the storage boundary back to their
// workspace-domain authorization API without parsing lock keys.
func ParseObject(value string) (Resource, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 5 || parts[0] != "flashyun" || parts[1] != "file" {
		return Resource{}, errInvalidResource
	}
	values := make([]string, 3)
	for index, part := range parts[2:] {
		decoded, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil || encodeScope(string(decoded)) != part {
			return Resource{}, errInvalidResource
		}
		values[index] = string(decoded)
	}
	resource := Resource{Tenant: values[0], Workspace: values[1], File: values[2]}
	if err := resource.Validate(); err != nil {
		return Resource{}, err
	}
	return resource, nil
}

// LockKey returns the Redis key used to coordinate mutations for exactly one
// logical file. Each scope component is encoded independently to prevent
// delimiter-based collisions.
func (r Resource) LockKey() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	return "flashyun:storage:file:" + encodeScope(r.Tenant) + ":" + encodeScope(r.Workspace) + ":" + encodeScope(r.File), nil
}

func encodeScope(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

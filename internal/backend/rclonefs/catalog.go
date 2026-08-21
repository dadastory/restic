package rclonefs

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

	_ "github.com/rclone/rclone/backend/azureblob"
	_ "github.com/rclone/rclone/backend/b2"
	_ "github.com/rclone/rclone/backend/googlecloudstorage"
	_ "github.com/rclone/rclone/backend/local"
	_ "github.com/rclone/rclone/backend/s3"
	_ "github.com/rclone/rclone/backend/sftp"
	_ "github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/fs/config/configmap"
)

var ErrBackendNotAdmitted = errors.New("rclone backend is not admitted")

type PublicationMode uint8

const (
	publicationDirect PublicationMode = iota
	publicationTemporaryMove
)

type Capabilities struct {
	Backend           string
	RcloneBackend     string
	Publication       PublicationMode
	HasAtomicReplace  bool
	SupportsRangeRead bool
	AllowedOptions    map[string]struct{}
}

var admittedCatalog = map[string]Capabilities{
	"azureblob": {
		Backend: "azureblob", RcloneBackend: "azureblob", Publication: publicationDirect, HasAtomicReplace: true, SupportsRangeRead: true,
		AllowedOptions: optionSet("account", "key", "endpoint", "chunk_size", "upload_concurrency", "no_check_container"),
	},
	"b2": {
		Backend: "b2", RcloneBackend: "b2", Publication: publicationDirect, HasAtomicReplace: true, SupportsRangeRead: true,
		AllowedOptions: optionSet("account", "key", "endpoint", "chunk_size", "upload_concurrency", "hard_delete"),
	},
	"gcs": {
		Backend: "gcs", RcloneBackend: "google cloud storage", Publication: publicationDirect, HasAtomicReplace: true, SupportsRangeRead: true,
		AllowedOptions: optionSet("project_number", "service_account_credentials", "endpoint", "bucket_policy_only", "no_check_bucket"),
	},
	"local": {
		Backend:           "local",
		RcloneBackend:     "local",
		Publication:       publicationTemporaryMove,
		HasAtomicReplace:  true,
		SupportsRangeRead: true,
		AllowedOptions: optionSet(
			"case_sensitive", "encoding", "no_check_updated", "no_preallocate", "no_set_modtime",
		),
	},
	"s3": {
		Backend:           "s3",
		RcloneBackend:     "s3",
		Publication:       publicationDirect,
		HasAtomicReplace:  true,
		SupportsRangeRead: true,
		AllowedOptions: optionSet(
			"access_key_id", "acl", "bucket_acl", "chunk_size", "endpoint",
			"force_path_style", "no_check_bucket", "provider", "region", "secret_access_key",
			"upload_concurrency", "upload_cutoff",
		),
	},
	"sftp": {
		Backend: "sftp", RcloneBackend: "sftp", Publication: publicationTemporaryMove, HasAtomicReplace: true, SupportsRangeRead: true,
		AllowedOptions: optionSet("host", "port", "user", "pass", "key_pem", "host_keys", "chunk_size", "disable_hashcheck"),
	},
	"webdav": {
		Backend: "webdav", RcloneBackend: "webdav", Publication: publicationTemporaryMove, HasAtomicReplace: true, SupportsRangeRead: true,
		AllowedOptions: optionSet("url", "vendor", "user", "pass", "bearer_token"),
	},
}

func optionSet(keys ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		result[key] = struct{}{}
	}
	return result
}

func AdmittedBackends() []string {
	names := make([]string, 0, len(admittedCatalog))
	for name := range admittedCatalog {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ValidateSnapshot validates a complete immutable provider snapshot without
// constructing a filesystem or performing network I/O.
func ValidateSnapshot(snapshot ProviderSnapshot) error {
	_, err := capabilityFor(snapshot)
	return err
}

func capabilityFor(snapshot ProviderSnapshot) (Capabilities, error) {
	capability, ok := admittedCatalog[snapshot.Backend]
	if !ok {
		return Capabilities{}, fmt.Errorf("%w: %s", ErrBackendNotAdmitted, snapshot.Backend)
	}
	if snapshot.Root == "" {
		return Capabilities{}, errors.New("repository root is required")
	}
	if snapshot.Backend == "local" && !filepath.IsAbs(snapshot.Root) {
		return Capabilities{}, errors.New("local repository root must be absolute")
	}
	if snapshot.Backend == "s3" {
		for _, key := range []string{"provider", "endpoint", "access_key_id", "secret_access_key"} {
			if strings.TrimSpace(snapshot.Options[key]) == "" {
				return Capabilities{}, fmt.Errorf("repository option %q is required", key)
			}
		}
	}
	required := map[string][]string{
		"azureblob": {"account", "key"},
		"b2":        {"account", "key"},
		"gcs":       {"service_account_credentials"},
		"sftp":      {"host", "user", "host_keys"},
		"webdav":    {"url", "vendor"},
	}
	for _, key := range required[snapshot.Backend] {
		if strings.TrimSpace(snapshot.Options[key]) == "" {
			return Capabilities{}, fmt.Errorf("repository option %q is required", key)
		}
	}
	if snapshot.Backend == "sftp" && strings.TrimSpace(snapshot.Options["pass"]) == "" && strings.TrimSpace(snapshot.Options["key_pem"]) == "" {
		return Capabilities{}, errors.New("sftp password or inline private key is required")
	}
	if snapshot.Backend == "webdav" && strings.TrimSpace(snapshot.Options["pass"]) == "" && strings.TrimSpace(snapshot.Options["bearer_token"]) == "" {
		return Capabilities{}, errors.New("webdav password or bearer token is required")
	}
	for key, value := range snapshot.Options {
		if _, ok := capability.AllowedOptions[key]; !ok {
			return Capabilities{}, fmt.Errorf("repository option %q is not admitted", key)
		}
		if strings.ContainsAny(key, "\r\n") || strings.ContainsAny(value, "\r\n") {
			return Capabilities{}, fmt.Errorf("repository option %q contains a line break", key)
		}
	}
	return capability, nil
}

type immutableMapper struct {
	values  map[string]string
	mu      sync.Mutex
	mutated []string
}

var _ configmap.Mapper = (*immutableMapper)(nil)

func newImmutableMapper(values map[string]string) *immutableMapper {
	copyOfValues := make(map[string]string, len(values))
	for key, value := range values {
		copyOfValues[key] = value
	}
	return &immutableMapper{values: copyOfValues}
}

func (mapper *immutableMapper) Get(key string) (string, bool) {
	value, ok := mapper.values[key]
	return value, ok
}

func (mapper *immutableMapper) Set(key, _ string) {
	mapper.mu.Lock()
	defer mapper.mu.Unlock()
	if !slices.Contains(mapper.mutated, key) {
		mapper.mutated = append(mapper.mutated, key)
		sort.Strings(mapper.mutated)
	}
}

func (mapper *immutableMapper) Mutated() bool {
	mapper.mu.Lock()
	defer mapper.mu.Unlock()
	return len(mapper.mutated) != 0
}

func (mapper *immutableMapper) MutatedKeys() []string {
	mapper.mu.Lock()
	defer mapper.mu.Unlock()
	return slices.Clone(mapper.mutated)
}

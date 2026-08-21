// Package resticstore is FlashYun's public, typed integration boundary for its
// pinned Restic fork. Applications import this package rather than Restic
// command packages or internal packages.
package resticstore

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/restic/restic/internal/backend/rclonefs"
)

var ErrInvalidProvider = errors.New("repository provider is required")

// Config contains the secret-bearing configuration required to access one
// Restic repository. Callers must retain it privately and use Redacted when
// creating logs, errors, audit records, or API responses.
type Config struct {
	Provider           Provider
	RepositoryPassword string
	// CacheDirectory is a server-controlled root for Restic's repository-ID
	// isolated metadata cache. Empty disables caching for tests or specialized
	// consumers; ordinary file-data packs are never auto-cached by Restic.
	CacheDirectory string
}

// Provider describes exactly one catalog-validated repository provider.
type Provider struct {
	// RuntimeIdentity is a secret-free immutable provider/workspace/revision key
	// used only for bounded in-process filesystem reuse.
	RuntimeIdentity string
	// Repository is the immutable generic repository-provider snapshot resolved
	// by Storage.
	Repository *RepositoryProvider
}

// RepositoryProvider contains one complete, already validated runtime
// projection. Options remain secret-bearing and must never cross Storage's
// process boundary.
type RepositoryProvider struct {
	Backend string
	Root    string
	Options map[string]string
}

// AdmittedRepositoryBackends returns the explicitly compiled repository
// catalog. It does not reflect arbitrary rclone registrations.
func AdmittedRepositoryBackends() []string { return rclonefs.AdmittedBackends() }

// ProviderSummary is safe to expose outside the storage process.
type ProviderSummary struct {
	Backend string
}

// String intentionally renders only safe provider identity fields.
func (s ProviderSummary) String() string {
	if s.Backend == "" {
		return "provider=unknown"
	}
	return fmt.Sprintf("provider=%s", s.Backend)
}

// Validate rejects incomplete, ambiguous, or unsafe provider settings without
// including secret values in an error.
func (c Config) Validate() error {
	if c.RepositoryPassword == "" {
		return errors.New("restic repository password is required")
	}
	if c.CacheDirectory != "" {
		cacheDirectory := filepath.Clean(c.CacheDirectory)
		if !filepath.IsAbs(cacheDirectory) || cacheDirectory == string(filepath.Separator) {
			return errors.New("restic cache directory must be a private absolute path")
		}
	}

	if c.Provider.Repository == nil {
		return ErrInvalidProvider
	}
	repository := c.Provider.Repository
	if err := rclonefs.ValidateSnapshot(rclonefs.ProviderSnapshot{
		Backend: repository.Backend, Root: repository.Root, Options: repository.Options,
	}); err != nil {
		return fmt.Errorf("invalid repository provider: %w", err)
	}
	return nil
}

// Redacted returns the safe identity representation of a provider. It omits
// provider credentials and the repository password by construction.
func (c Config) Redacted() ProviderSummary {
	if c.Provider.Repository == nil {
		return ProviderSummary{}
	}
	return ProviderSummary{Backend: c.Provider.Repository.Backend}
}

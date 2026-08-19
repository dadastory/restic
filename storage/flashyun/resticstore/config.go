// Package resticstore is FlashYun's public, typed integration boundary for its
// pinned Restic fork. Applications import this package rather than Restic
// command packages or internal packages.
package resticstore

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
)

// ProviderKind identifies a natively supported backend type.
type ProviderKind string

const (
	// ProviderLocal is restricted to development and test deployments by the API.
	ProviderLocal ProviderKind = "local"
	// ProviderS3 is a native S3-compatible storage provider.
	ProviderS3 ProviderKind = "s3"
)

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

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

// Provider describes exactly one typed storage provider.
type Provider struct {
	Kind  ProviderKind
	Local *LocalProvider
	S3    *S3Provider
}

// LocalProvider describes a local filesystem repository.
type LocalProvider struct {
	Path string
}

// S3Provider describes an S3-compatible repository location and static
// credentials. Static credentials are deliberately scoped to this provider;
// no process environment mutation is required for concurrent providers.
type S3Provider struct {
	Endpoint  string
	UseHTTP   bool
	Bucket    string
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
	Transport http.RoundTripper
}

// ProviderSummary is safe to expose outside the storage process.
type ProviderSummary struct {
	Kind     ProviderKind
	Path     string
	Endpoint string
	UseHTTP  bool
	Bucket   string
	Prefix   string
	Region   string
}

// String intentionally renders only safe provider identity fields.
func (s ProviderSummary) String() string {
	switch s.Kind {
	case ProviderLocal:
		return fmt.Sprintf("provider=local path=%q", s.Path)
	case ProviderS3:
		return fmt.Sprintf("provider=s3 endpoint=%q bucket=%q prefix=%q", s.Endpoint, s.Bucket, s.Prefix)
	default:
		return "provider=unknown"
	}
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

	switch c.Provider.Kind {
	case ProviderLocal:
		if c.Provider.Local == nil || c.Provider.S3 != nil {
			return errors.New("local provider must define only local settings")
		}
		path := filepath.Clean(c.Provider.Local.Path)
		if !filepath.IsAbs(path) || path == "." {
			return errors.New("local provider path must be absolute")
		}
	case ProviderS3:
		if c.Provider.S3 == nil || c.Provider.Local != nil {
			return errors.New("S3 provider must define only S3 settings")
		}
		s3 := c.Provider.S3
		if s3.Endpoint == "" || strings.Contains(s3.Endpoint, "://") || strings.ContainsAny(s3.Endpoint, "/?#@ ") {
			return errors.New("S3 endpoint must be a host and optional port")
		}
		if !bucketPattern.MatchString(s3.Bucket) {
			return errors.New("S3 bucket is invalid")
		}
		if strings.HasPrefix(s3.Prefix, "/") || strings.Contains(s3.Prefix, "..") {
			return errors.New("S3 prefix is invalid")
		}
		if s3.AccessKey == "" || s3.SecretKey == "" || s3.Transport == nil {
			return errors.New("S3 credentials and guarded transport are required")
		}
	default:
		return errors.New("storage provider kind is required")
	}
	return nil
}

// Redacted returns the safe identity representation of a provider. It omits
// provider credentials and the repository password by construction.
func (c Config) Redacted() ProviderSummary {
	summary := ProviderSummary{Kind: c.Provider.Kind}
	if c.Provider.Local != nil {
		summary.Path = c.Provider.Local.Path
	}
	if c.Provider.S3 != nil {
		summary.Endpoint = c.Provider.S3.Endpoint
		summary.UseHTTP = c.Provider.S3.UseHTTP
		summary.Bucket = c.Provider.S3.Bucket
		summary.Prefix = c.Provider.S3.Prefix
		summary.Region = c.Provider.S3.Region
	}
	return summary
}

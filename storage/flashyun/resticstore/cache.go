package resticstore

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	backendcache "github.com/restic/restic/internal/backend/cache"
)

// CleanupCache removes only official repository-ID cache directories older
// than maxAge. backendcache.OlderThan filters entries to 64-hex repository IDs,
// preventing broad or caller-controlled path removal.
func CleanupCache(directory string, maxAge time.Duration) error {
	directory = filepath.Clean(directory)
	if !filepath.IsAbs(directory) || directory == string(filepath.Separator) || maxAge <= 0 {
		return errors.New("resticstore cache cleanup boundary is invalid")
	}
	entries, err := backendcache.OlderThan(directory, maxAge)
	if err != nil {
		return errors.New("resticstore cache cleanup unavailable")
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(directory, entry.Name())); err != nil {
			return errors.New("resticstore cache cleanup unavailable")
		}
	}
	return nil
}

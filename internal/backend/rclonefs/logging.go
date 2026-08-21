package rclonefs

import (
	"io"
	"log/slog"
	"sync"

	"github.com/rclone/rclone/fs"
)

var installSafeLogger sync.Once

func ensureSafeLogging() {
	// Provider errors are translated into FlashYun's stable stage/error
	// categories at the adapter boundary. rclone's global logger can include
	// remote names or backend-specific diagnostics, so raw third-party output
	// is discarded instead of entering the service log pipeline.
	installSafeLogger.Do(func() {
		fs.SetLogger(slog.NewTextHandler(io.Discard, nil))
	})
}

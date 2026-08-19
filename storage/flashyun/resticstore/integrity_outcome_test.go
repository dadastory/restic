package resticstore

import (
	"context"
	"errors"
	"testing"

	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

func TestIntegrityClassificationSeparatesAvailabilityFromVerifiedCorruption(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		code      string
		retryable bool
		corrupt   bool
	}{
		{name: "timeout", err: context.DeadlineExceeded, code: IntegrityCodeTimeout, retryable: true},
		{name: "unknown backend", err: errors.New("secret endpoint failure"), code: IntegrityCodeCheckUnavailable, retryable: true},
		{name: "pack metadata", err: &repository.ErrPackMetadata{ID: restic.ID{1}, Missing: true, Err: errors.New("missing")}, code: IntegrityCodeCorrupt, corrupt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyIntegrityCheckError(context.Background(), tt.err, IntegrityCodeCheckUnavailable)
			failure, ok := err.(*IntegrityError)
			if !ok {
				t.Fatalf("expected *IntegrityError, got %T", err)
			}
			if failure.IntegrityCode() != tt.code || failure.IntegrityRetryable() != tt.retryable || failure.CorruptionVerified() != tt.corrupt {
				t.Fatalf("unexpected classification: %#v", failure)
			}
			if errors.Is(err, tt.err) {
				t.Fatal("redacted integrity outcomes must not unwrap raw backend errors")
			}
		})
	}
}

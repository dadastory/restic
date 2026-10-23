package resticstore

import (
	"context"
	"errors"
	"fmt"
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
		{name: "orphaned pack", err: &repository.ErrPackMetadata{ID: restic.ID{1}, Orphaned: true, Err: errors.New("unindexed")}, code: IntegrityCodeCheckUnavailable, retryable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyIntegrityCheckError(context.Background(), tt.err, IntegrityCodeCheckUnavailable)
			failure := new(IntegrityError)
			if !errors.As(err, &failure) {
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

func TestFirstCheckErrorPreservesFatalIndexOutcomes(t *testing.T) {
	duplicate := &repository.ErrDuplicatePacks{}
	mixed := &repository.ErrMixedPack{}
	incomplete := &repository.ErrIncompletePackEntry{}
	unknown := errors.New("unknown hint")
	failure := errors.New("index read failed")
	for _, tt := range []struct {
		name  string
		hints []error
		errs  []error
		want  error
	}{
		{name: "clean"},
		{name: "native maintenance hints", hints: []error{duplicate, fmt.Errorf("wrapped: %w", mixed)}},
		{name: "incomplete index", hints: []error{duplicate, incomplete}, want: incomplete},
		{name: "unknown hint", hints: []error{mixed, unknown}, want: unknown},
		{name: "index failure", hints: []error{duplicate}, errs: []error{failure}, want: failure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := firstCheckError(tt.hints, tt.errs); !errors.Is(got, tt.want) {
				t.Fatalf("firstCheckError = %v, want %v", got, tt.want)
			}
		})
	}
}

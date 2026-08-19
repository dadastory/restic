package resticstore

import (
	"errors"
	"testing"

	"github.com/restic/restic/internal/repository"
)

func TestSnapshotArchiverAlwaysPropagatesWorkerErrorsWithoutPanicking(t *testing.T) {
	archive := newSnapshotArchiver((*repository.Repository)(nil), SnapshotRequest{})
	if archive.Error == nil {
		t.Fatal("snapshot archiver Error callback is nil")
	}
	want := errors.New("read failed")
	if got := archive.Error("file", want); !errors.Is(got, want) {
		t.Fatalf("snapshot archiver Error callback returned %v, want %v", got, want)
	}
}

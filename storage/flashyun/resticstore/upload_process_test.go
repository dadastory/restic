package resticstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestUploadProcessRestartAfterCommittedCheckpoint(t *testing.T) {
	payload := uploadTestPayload(12<<20 + 71)
	if root := os.Getenv("FLASHYUN_UPLOAD_CRASH_TEST_ROOT"); root != "" {
		store, err := New(Config{Provider: testLocalProvider(root), RepositoryPassword: "repository-password"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = store.AppendUpload(t.Context(), UploadAppendRequest{
			UploadID: "process-upload", ExpectedSize: int64(len(payload)), CheckpointBytes: 2 << 20, Source: bytes.NewReader(payload),
			Commit: func(_ context.Context, candidate UploadCheckpoint) error {
				// Persist the caller's acknowledged head, then die before append
				// returns or releases its lock, as with a lost PATCH response.
				if err := json.NewEncoder(os.Stdout).Encode(candidate); err != nil {
					return err
				}
				os.Exit(23)
				return nil
			},
		})
		t.Fatalf("child did not exit from committed checkpoint: %v", err)
	}
	root := filepath.Join(t.TempDir(), "repository")
	store := initializedReaderStore(t, root)
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestUploadProcessRestartAfterCommittedCheckpoint$")
	child.Env = append(os.Environ(), "FLASHYUN_UPLOAD_CRASH_TEST_ROOT="+root)
	var stderr bytes.Buffer
	child.Stderr = &stderr
	output, err := child.Output()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 23 {
		t.Fatalf("checkpoint child failed: %v: %s", err, stderr.String())
	}
	var head UploadCheckpoint
	if err := json.Unmarshal(output, &head); err != nil {
		t.Fatal(err)
	}
	if head.Offset <= 0 || head.Offset >= int64(len(payload)) {
		t.Fatalf("invalid interrupted head: %+v", head)
	}
	// A dead append process must not indefinitely block read-only integrity
	// checking. Recover only locks that native Restic proves stale/dead.
	checkCtx, cancelCheck := context.WithTimeout(t.Context(), 5*time.Second)
	checkErr := store.Check(checkCtx)
	cancelCheck()
	if checkErr != nil {
		t.Fatalf("check after dead append owner: %v", checkErr)
	}
	assertUploadBytes(t, store, "process-upload", head, payload[:head.Offset])
	resumed, err := store.AppendUpload(t.Context(), UploadAppendRequest{UploadID: "process-upload", PreviousSnapshotID: head.SnapshotID, Offset: head.Offset, ExpectedSize: int64(len(payload)), CheckpointBytes: 2 << 20, Source: bytes.NewReader(payload[head.Offset:]), Commit: acceptUpload})
	if err != nil {
		t.Fatal(err)
	}
	assertUploadBytes(t, store, "process-upload", resumed, payload)
	continuous, err := store.SnapshotReader(t.Context(), readerBatchRequests([][]byte{payload}, "process-continuous")[0])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(uploadContentIDs(t, store, resumed.SnapshotID), uploadContentIDs(t, store, continuous.ID)) {
		t.Fatal("process restart changed content-defined chunks")
	}
	// The child bypassed unlock. Cleanup must use native stale-lock detection
	// and preserve the interrupted upload while cleaning an unrelated identity.
	if err := store.CleanupUpload(t.Context(), "unrelated-upload"); err != nil {
		t.Fatal(err)
	}
	assertUploadBytes(t, store, "process-upload", resumed, payload)
	if err := store.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotRecoversNativeLockAfterExclusiveOwnerDies(t *testing.T) {
	if root := os.Getenv("FLASHYUN_EXCLUSIVE_CRASH_TEST_ROOT"); root != "" {
		store, err := New(Config{Provider: testLocalProvider(root), RepositoryPassword: "repository-password"})
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, err = store.openLockedRepository(t.Context(), true)
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(23)
	}
	root := filepath.Join(t.TempDir(), "repository")
	store := initializedReaderStore(t, root)
	child := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSnapshotRecoversNativeLockAfterExclusiveOwnerDies$")
	child.Env = append(os.Environ(), "FLASHYUN_EXCLUSIVE_CRASH_TEST_ROOT="+root)
	output, err := child.CombinedOutput()
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 23 {
		t.Fatalf("exclusive owner child failed: %v: %s", err, output)
	}
	request := readerBatchRequests([][]byte{[]byte("payload")}, "after-dead-exclusive")[0]
	if _, err := store.SnapshotReader(t.Context(), request); err != nil {
		t.Fatalf("snapshot after exclusive lock owner died: %v", err)
	}
	repo, _, unlock, err := store.openLockedRepository(t.Context(), true)
	if err != nil {
		t.Fatalf("exclusive lock after successful snapshot: %v", err)
	}
	unlock()
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotBatchDoesNotReadIndexWhileExclusiveMaintenanceOwnsRepository(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	repo, _, unlock, err := store.openLockedRepository(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unlock(); _ = repo.Close() }()
	indexLoaded := false
	store.observeReaderBatchSession = func(stage string) {
		if stage == "index_loaded" {
			indexLoaded = true
		}
	}
	request := ReaderSnapshotBatchRequest{Items: readerBatchRequests([][]byte{[]byte("payload")}, "during-maintenance")}
	if _, err := store.SnapshotReadersBatch(t.Context(), request); err == nil {
		t.Fatal("snapshot ran while exclusive maintenance was active")
	}
	if indexLoaded {
		t.Fatal("snapshot loaded an unprotected index while exclusive maintenance was active")
	}
}

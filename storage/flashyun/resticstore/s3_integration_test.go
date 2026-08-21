//go:build integration

package resticstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestS3StoreConcurrentBatchRestoreProfileAgainstMinIO(t *testing.T) {
	if os.Getenv("FLASHYUN_RESTICSTORE_ARCHIVE_PROFILE") != "1" {
		t.Skip("set FLASHYUN_RESTICSTORE_ARCHIVE_PROFILE=1")
	}
	endpoint := os.Getenv("FLASHYUN_RESTICSTORE_S3_ENDPOINT")
	accessKey := os.Getenv("FLASHYUN_RESTICSTORE_S3_ACCESS_KEY")
	secretKey := os.Getenv("FLASHYUN_RESTICSTORE_S3_SECRET_KEY")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("FlashYun S3 facade integration environment is not configured")
	}
	store, err := New(Config{
		Provider:           testS3Provider(endpoint, true, "flashyun-resticstore-test", "archive-profile/"+time.Now().UTC().Format("20060102150405.000000000"), "", accessKey, secretKey),
		RepositoryPassword: "flashyun-resticstore-archive-profile-password",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	payloads := deterministicPayloads(32, 32<<10)
	snapshots, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: readerBatchRequests(payloads, "minio-archive-profile")})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	var wait sync.WaitGroup
	errorsByCohort := make([]error, 2)
	for cohort := 0; cohort < 2; cohort++ {
		cohort := cohort
		wait.Add(1)
		go func() {
			defer wait.Done()
			items := make([]RestoreBatchItem, 0, 16)
			for index := cohort * 16; index < (cohort+1)*16; index++ {
				items = append(items, RestoreBatchItem{SnapshotID: snapshots.Items[index].Snapshot.ID, DestinationPath: filepath.Join(root, fmt.Sprintf("%02d", index)), ExpectedSize: int64(len(payloads[index]))})
			}
			result, restoreErr := store.RestoreSnapshotsBatch(ctx, RestoreBatchRequest{Items: items})
			if restoreErr == nil {
				for _, item := range result.Items {
					if item.Err != nil {
						restoreErr = item.Err
						break
					}
				}
			}
			errorsByCohort[cohort] = restoreErr
		}()
	}
	wait.Wait()
	for cohort, restoreErr := range errorsByCohort {
		if restoreErr != nil {
			t.Fatalf("cohort %d: %v", cohort, restoreErr)
		}
	}
	for index, expected := range payloads {
		actual, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("%02d", index), "payload"))
		if err != nil || string(actual) != string(expected) {
			t.Fatalf("payload %d mismatch: %v", index, err)
		}
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	cancelledDestination := filepath.Join(root, "cancelled")
	_, err = store.RestoreSnapshotsBatch(cancelled, RestoreBatchRequest{Items: []RestoreBatchItem{{
		SnapshotID: snapshots.Items[0].Snapshot.ID, DestinationPath: cancelledDestination, ExpectedSize: int64(len(payloads[0])),
	}}})
	if err == nil {
		t.Fatal("cancelled batch restore succeeded")
	}
	if _, statErr := os.Stat(filepath.Join(cancelledDestination, "payload")); !os.IsNotExist(statErr) {
		t.Fatalf("cancelled restore published payload: %v", statErr)
	}
}

func TestS3StoreSnapshotsRestoresAndMeasuresAgainstMinIO(t *testing.T) {
	endpoint := os.Getenv("FLASHYUN_RESTICSTORE_S3_ENDPOINT")
	accessKey := os.Getenv("FLASHYUN_RESTICSTORE_S3_ACCESS_KEY")
	secretKey := os.Getenv("FLASHYUN_RESTICSTORE_S3_SECRET_KEY")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Skip("FlashYun S3 facade integration environment is not configured")
	}

	store, err := New(Config{
		Provider:           testS3Provider(endpoint, true, "flashyun-resticstore-test", "facade/"+time.Now().UTC().Format("20060102150405.000000000"), "", accessKey, secretKey),
		RepositoryPassword: "flashyun-resticstore-integration-password",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	source := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "payload.txt"), []byte("S3 facade payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx, SnapshotRequest{SourcePath: source, Hostname: "flashyun-minio-integration"})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	restore := filepath.Join(t.TempDir(), "restore")
	if err := store.Restore(ctx, RestoreRequest{SnapshotID: snapshot.ID, DestinationPath: restore}); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if !findFilePayload(t, restore, "S3 facade payload") {
		t.Fatal("Restore() did not recover the S3 payload")
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.SnapshotCount != 1 || stats.RepositoryBytes == 0 {
		t.Fatalf("Stats() = %#v", stats)
	}
	if err := store.Check(ctx); err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	destination, err := New(Config{
		Provider:           testS3Provider(endpoint, true, "flashyun-resticstore-test", "facade-copy/"+time.Now().UTC().Format("20060102150405.000000000"), "", accessKey, secretKey),
		RepositoryPassword: "flashyun-resticstore-copy-password",
	})
	if err != nil {
		t.Fatalf("New(destination) error = %v", err)
	}
	if err := destination.Initialize(ctx); err != nil {
		t.Fatalf("destination Initialize() error = %v", err)
	}
	copied, err := store.Copy(ctx, destination, CopyRequest{SnapshotIDs: []string{snapshot.ID}})
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if copied.CopiedSnapshots != 1 || len(copied.SnapshotIDs) != 1 {
		t.Fatalf("Copy() result = %#v", copied)
	}
	copyRestore := filepath.Join(t.TempDir(), "copy-restore")
	if err := destination.Restore(ctx, RestoreRequest{SnapshotID: copied.SnapshotIDs[0], DestinationPath: copyRestore}); err != nil {
		t.Fatalf("destination Restore() error = %v", err)
	}
	if !findFilePayload(t, copyRestore, "S3 facade payload") {
		t.Fatal("copied S3 snapshot did not restore payload")
	}
	repeated, err := store.Copy(ctx, destination, CopyRequest{SnapshotIDs: []string{snapshot.ID}})
	if err != nil {
		t.Fatalf("repeat Copy() error = %v", err)
	}
	if repeated.CopiedSnapshots != 0 || repeated.SkippedSnapshots != 1 {
		t.Fatalf("repeat Copy() result = %#v", repeated)
	}

	retainedSnapshot, err := store.Snapshot(ctx, SnapshotRequest{SourcePath: source, Hostname: "flashyun-minio-integration", SnapshotTime: time.Now().UTC().Add(time.Second)})
	if err != nil {
		t.Fatalf("second Snapshot() error = %v", err)
	}
	retention, err := store.ForgetPrune(ctx, RetentionRequest{KeepLast: 1})
	if err != nil {
		t.Fatalf("ForgetPrune() error = %v", err)
	}
	if retention.KeptSnapshots != 1 || retention.ForgottenSnapshots != 1 {
		t.Fatalf("ForgetPrune() result = %#v", retention)
	}
	if err := store.Check(ctx); err != nil {
		t.Fatalf("Check() after ForgetPrune error = %v", err)
	}

	exactTarget, err := store.Snapshot(ctx, SnapshotRequest{SourcePath: source, Hostname: "flashyun-minio-integration", SnapshotTime: time.Now().UTC().Add(2 * time.Second)})
	if err != nil {
		t.Fatalf("exact-target Snapshot() error = %v", err)
	}
	exactRequest := RetentionRequest{
		ForgetSnapshotIDs:    []string{exactTarget.ID},
		ProtectedSnapshotIDs: []string{retainedSnapshot.ID},
	}
	exact, err := store.ForgetPrune(ctx, exactRequest)
	if err != nil {
		t.Fatalf("exact ForgetPrune() error = %v", err)
	}
	if exact.ForgottenSnapshots != 1 {
		t.Fatalf("exact ForgetPrune() result = %#v", exact)
	}
	if err := store.Restore(ctx, RestoreRequest{SnapshotID: exactTarget.ID, DestinationPath: filepath.Join(t.TempDir(), "forgotten")}); err == nil {
		t.Fatal("forgotten exact S3 snapshot remained restorable")
	}
	if err := store.Restore(ctx, RestoreRequest{SnapshotID: retainedSnapshot.ID, DestinationPath: filepath.Join(t.TempDir(), "protected")}); err != nil {
		t.Fatalf("protected S3 snapshot Restore() error = %v", err)
	}
	resumed, err := store.ForgetPrune(ctx, exactRequest)
	if err != nil {
		t.Fatalf("resumed exact ForgetPrune() error = %v", err)
	}
	if resumed.ForgottenSnapshots != 1 {
		t.Fatalf("resumed exact ForgetPrune() result = %#v", resumed)
	}
}

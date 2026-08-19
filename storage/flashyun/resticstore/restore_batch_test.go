package resticstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRestoreSnapshotsBatchReusesRepositorySessionAndIsolatesItems(t *testing.T) {
	ctx := context.Background()
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	payloads := deterministicPayloads(3, 1024)
	snapshots, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: readerBatchRequests(payloads, "restore-batch")})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	events := make(map[string]int)
	store.observeRestoreBatchSession = func(stage string) { events[stage]++ }
	items := make([]RestoreBatchItem, len(payloads))
	for index, item := range snapshots.Items {
		items[index] = RestoreBatchItem{SnapshotID: item.Snapshot.ID, DestinationPath: filepath.Join(root, string(rune('a'+index))), ExpectedSize: int64(len(payloads[index]))}
	}
	result, err := store.RestoreSnapshotsBatch(ctx, RestoreBatchRequest{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	for index, item := range result.Items {
		if item.Err != nil {
			t.Fatalf("item %d: %v", index, item.Err)
		}
		payload, err := os.ReadFile(filepath.Join(items[index].DestinationPath, "payload"))
		if err != nil || string(payload) != string(payloads[index]) {
			t.Fatalf("item %d payload mismatch: err=%v", index, err)
		}
	}
	for _, stage := range []string{"repository_opened", "repository_locked", "index_loaded"} {
		if events[stage] != 1 {
			t.Fatalf("%s count = %d, want 1", stage, events[stage])
		}
	}
}

func TestRestoreSnapshotsBatchRejectsBoundsBeforeRepositoryOpen(t *testing.T) {
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	var opened int
	store.observeRestoreBatchSession = func(string) { opened++ }
	destination := filepath.Join(t.TempDir(), "same")
	_, err := store.RestoreSnapshotsBatch(context.Background(), RestoreBatchRequest{Items: []RestoreBatchItem{
		{SnapshotID: "invalid", DestinationPath: destination, ExpectedSize: 1},
		{SnapshotID: "invalid", DestinationPath: destination, ExpectedSize: 1},
	}})
	if err == nil || opened != 0 {
		t.Fatalf("err=%v opened=%d", err, opened)
	}
}

func TestRestoreSnapshotsBatchKeepsPerItemFailureIndependent(t *testing.T) {
	ctx := context.Background()
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	payload := deterministicPayloads(1, 1024)
	snapshots, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: readerBatchRequests(payload, "restore-independent")})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	items := []RestoreBatchItem{
		{SnapshotID: strings.Repeat("a", 64), DestinationPath: filepath.Join(root, "missing"), ExpectedSize: 1},
		{SnapshotID: snapshots.Items[0].Snapshot.ID, DestinationPath: filepath.Join(root, "healthy"), ExpectedSize: int64(len(payload[0]))},
	}
	result, err := store.RestoreSnapshotsBatch(ctx, RestoreBatchRequest{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	if result.Items[0].Err != ErrRestoreBatchItemUnavailable {
		t.Fatalf("missing snapshot item error = %v", result.Items[0].Err)
	}
	if _, err := os.Stat(items[0].DestinationPath); !os.IsNotExist(err) {
		t.Fatalf("failed item destination remains: %v", err)
	}
	if result.Items[1].Err != nil {
		t.Fatalf("healthy item failed: %v", result.Items[1].Err)
	}
	actual, err := os.ReadFile(filepath.Join(items[1].DestinationPath, "payload"))
	if err != nil || string(actual) != string(payload[0]) {
		t.Fatalf("healthy payload mismatch: %v", err)
	}
}

func TestRestoreSnapshotsBatchCancellationRemovesIncompleteTargets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	payloads := deterministicPayloads(2, 1024)
	snapshots, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: readerBatchRequests(payloads, "restore-cancel")})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	items := make([]RestoreBatchItem, len(payloads))
	for index := range items {
		items[index] = RestoreBatchItem{SnapshotID: snapshots.Items[index].Snapshot.ID, DestinationPath: filepath.Join(root, fmt.Sprintf("%d", index)), ExpectedSize: int64(len(payloads[index]))}
		if err := os.MkdirAll(items[index].DestinationPath, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(items[index].DestinationPath, "partial"), []byte("not published"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store.observeRestoreBatchSession = func(stage string) {
		if stage == "index_loaded" {
			cancel()
		}
	}
	result, err := store.RestoreSnapshotsBatch(ctx, RestoreBatchRequest{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	for index, item := range result.Items {
		if item.Err == nil {
			t.Fatalf("cancelled item %d succeeded", index)
		}
		if _, err := os.Stat(items[index].DestinationPath); !os.IsNotExist(err) {
			t.Fatalf("cancelled item %d destination remains: %v", index, err)
		}
	}
}

func TestRestoreSnapshotsBatchLocalProfile(t *testing.T) {
	profile := os.Getenv("FLASHYUN_RESTIC_RESTORE_BATCH_PROFILE")
	if profile == "" {
		t.Skip("set FLASHYUN_RESTIC_RESTORE_BATCH_PROFILE=smoke or full")
	}
	if profile != "smoke" && profile != "full" {
		t.Fatal("FLASHYUN_RESTIC_RESTORE_BATCH_PROFILE must be smoke or full")
	}
	type dataset struct {
		name     string
		count    int
		itemSize int
	}
	datasets := []dataset{{name: "100-small", count: 100, itemSize: 4 << 10}}
	if profile == "full" {
		datasets = append(datasets,
			dataset{name: "1000-small", count: 1000, itemSize: 4 << 10},
			dataset{name: "4-large", count: 4, itemSize: 32 << 20},
		)
	}
	for _, current := range datasets {
		current := current
		t.Run(current.name, func(t *testing.T) {
			ctx := context.Background()
			store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
			payloads := deterministicPayloads(current.count, current.itemSize)
			requests := readerBatchRequests(payloads, "restore-profile-"+current.name)
			snapshotItems := make([]ReaderSnapshotItemResult, 0, len(requests))
			for start := 0; start < len(requests); start += MaxReaderSnapshotBatchItems {
				end := min(start+MaxReaderSnapshotBatchItems, len(requests))
				snapshots, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: requests[start:end]})
				if err != nil {
					t.Fatal(err)
				}
				snapshotItems = append(snapshotItems, snapshots.Items...)
			}
			legacyRoot := filepath.Join(t.TempDir(), "legacy")
			legacySessions := 0
			store.observeRestoreBatchSession = nil
			legacyStarted := time.Now()
			for index, item := range snapshotItems {
				if item.Err != nil {
					t.Fatalf("snapshot item %d: %v", index, item.Err)
				}
				legacySessions++
				if err := store.Restore(ctx, RestoreRequest{SnapshotID: item.Snapshot.ID, DestinationPath: filepath.Join(legacyRoot, fmt.Sprintf("%04d", index))}); err != nil {
					t.Fatalf("legacy restore %d: %v", index, err)
				}
			}
			legacyElapsed := time.Since(legacyStarted)

			batchRoot := filepath.Join(t.TempDir(), "batch")
			batchSessions := 0
			store.observeRestoreBatchSession = func(stage string) {
				if stage == "repository_opened" {
					batchSessions++
				}
			}
			batchStarted := time.Now()
			maximumStaging := int64(0)
			for start := 0; start < len(snapshotItems); start += MaxRestoreBatchItems {
				end := min(start+MaxRestoreBatchItems, len(snapshotItems))
				items := make([]RestoreBatchItem, 0, end-start)
				staging := int64(0)
				for index := start; index < end; index++ {
					staging += int64(len(payloads[index]))
					items = append(items, RestoreBatchItem{SnapshotID: snapshotItems[index].Snapshot.ID, DestinationPath: filepath.Join(batchRoot, fmt.Sprintf("%04d", index)), ExpectedSize: int64(len(payloads[index]))})
				}
				maximumStaging = max(maximumStaging, staging)
				result, err := store.RestoreSnapshotsBatch(ctx, RestoreBatchRequest{Items: items})
				if err != nil {
					t.Fatal(err)
				}
				for index, item := range result.Items {
					if item.Err != nil {
						t.Fatalf("batch item %d: %v", start+index, item.Err)
					}
				}
			}
			batchElapsed := time.Since(batchStarted)
			for index, expected := range payloads {
				legacy, err := os.ReadFile(filepath.Join(legacyRoot, fmt.Sprintf("%04d", index), "payload"))
				if err != nil || string(legacy) != string(expected) {
					t.Fatalf("legacy payload %d mismatch: %v", index, err)
				}
				batch, err := os.ReadFile(filepath.Join(batchRoot, fmt.Sprintf("%04d", index), "payload"))
				if err != nil || string(batch) != string(expected) {
					t.Fatalf("batch payload %d mismatch: %v", index, err)
				}
			}
			t.Logf("dataset=%s files=%d logical_bytes=%d legacy_elapsed=%s legacy_sessions=%d batch_elapsed=%s batch_sessions=%d maximum_plaintext_staging=%d", current.name, current.count, int64(current.count*current.itemSize), legacyElapsed, legacySessions, batchElapsed, batchSessions, maximumStaging)
		})
	}
}

package resticstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestStoreSnapshotsReaderBatchAsIndependentSnapshots(t *testing.T) {
	ctx := context.Background()
	repositoryPath := filepath.Join(t.TempDir(), "repository")
	store := initializedReaderStore(t, repositoryPath)
	payloads := [][]byte{[]byte("first independent payload"), []byte("second independent payload"), []byte("first independent payload")}
	requests := readerBatchRequests(payloads, "independent")
	result, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: requests})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != len(payloads) {
		t.Fatalf("items = %d, want %d", len(result.Items), len(payloads))
	}
	ids := make(map[string]struct{}, len(payloads))
	for index, item := range result.Items {
		if item.Err != nil || item.Snapshot.ID == "" {
			t.Fatalf("item %d = %#v", index, item)
		}
		if _, duplicate := ids[item.Snapshot.ID]; duplicate {
			t.Fatalf("item %d reused snapshot %s", index, item.Snapshot.ID)
		}
		ids[item.Snapshot.ID] = struct{}{}
		hash := sha256.Sum256(payloads[index])
		if _, err := store.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{SnapshotID: item.Snapshot.ID, ExpectedSize: int64(len(payloads[index])), ExpectedChecksum: "sha256:" + hex.EncodeToString(hash[:])}); err != nil {
			t.Fatalf("verify item %d: %v", index, err)
		}
	}
	if packs := countReaderRepositoryPacks(t, repositoryPath); packs >= len(payloads)*2 {
		t.Fatalf("batch created %d packs for %d items; uploader flush was not shared", packs, len(payloads))
	}
}

func TestStoreReaderBatchIsolatesSourceFailure(t *testing.T) {
	ctx := context.Background()
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	requests := readerBatchRequests([][]byte{[]byte("good"), []byte("bad")}, "source-failure")
	requests[1].Size++
	result, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: requests})
	if err != nil {
		t.Fatal(err)
	}
	if result.Items[0].Err != nil || result.Items[0].Snapshot.ID == "" {
		t.Fatalf("valid item failed: %#v", result.Items[0])
	}
	if result.Items[1].Err == nil || result.Items[1].Snapshot.ID != "" {
		t.Fatalf("invalid item was published: %#v", result.Items[1])
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SnapshotCount != 1 {
		t.Fatalf("snapshot count = %d, want 1", stats.SnapshotCount)
	}
}

func TestStoreReaderBatchReconcilesExactOperationTags(t *testing.T) {
	ctx := context.Background()
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	requests := readerBatchRequests([][]byte{[]byte("alpha"), []byte("beta")}, "reconcile")
	first, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: requests})
	if err != nil {
		t.Fatal(err)
	}
	for index := range requests {
		requests[index].ReconcileExisting = true
	}
	second, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: requests})
	if err != nil {
		t.Fatal(err)
	}
	for index := range requests {
		if first.Items[index].Snapshot.ID == "" || second.Items[index].Snapshot.ID != first.Items[index].Snapshot.ID {
			t.Fatalf("item %d was not reconciled: first=%#v second=%#v", index, first.Items[index], second.Items[index])
		}
	}
}

func TestStoreReaderBatchRejectsBoundsBeforeOpeningSources(t *testing.T) {
	ctx := context.Background()
	store := initializedReaderStore(t, filepath.Join(t.TempDir(), "repository"))
	var opens atomic.Int64
	request := ReaderSnapshotRequest{Name: "payload", Size: 1, Hostname: "flashyun-reader-test", IdempotencyKey: "duplicate-operation", Open: func(context.Context) (io.ReadCloser, error) {
		opens.Add(1)
		return io.NopCloser(bytes.NewReader([]byte("x"))), nil
	}}
	_, err := store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: []ReaderSnapshotRequest{request, request}})
	if err == nil {
		t.Fatal("duplicate operation was accepted")
	}
	if opens.Load() != 0 {
		t.Fatalf("opened %d sources before validating the batch", opens.Load())
	}
}

func TestStoreReaderBatchHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store, err := New(Config{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: filepath.Join(t.TempDir(), "repository")}}, RepositoryPassword: "repository-password"})
	if err != nil {
		t.Fatal(err)
	}
	var opens atomic.Int64
	_, err = store.SnapshotReadersBatch(ctx, ReaderSnapshotBatchRequest{Items: []ReaderSnapshotRequest{{Name: "payload", Size: 1, Hostname: "flashyun-reader-test", IdempotencyKey: "cancelled-operation", Open: func(context.Context) (io.ReadCloser, error) {
		opens.Add(1)
		return io.NopCloser(bytes.NewReader([]byte("x"))), nil
	}}}})
	if err == nil || opens.Load() != 0 {
		t.Fatalf("cancelled batch err=%v opens=%d", err, opens.Load())
	}
}

func TestReaderSnapshotPerformanceComparison(t *testing.T) {
	if os.Getenv("FLASHYUN_RESTIC_BATCH_BENCHMARK") != "1" {
		t.Skip("set FLASHYUN_RESTIC_BATCH_BENCHMARK=1 to run the Restic ingest comparison")
	}
	cases := []struct {
		name     string
		payloads [][]byte
	}{
		{name: "100-small", payloads: deterministicPayloads(100, 4<<10)},
		{name: "4-large", payloads: deterministicPayloads(4, 8<<20)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			legacyPath := filepath.Join(t.TempDir(), "legacy")
			batchPath := filepath.Join(t.TempDir(), "batch")
			legacy := initializedReaderStore(t, legacyPath)
			batch := initializedReaderStore(t, batchPath)
			legacyRequests := readerBatchRequests(testCase.payloads, testCase.name+"-legacy")
			started := time.Now()
			legacyResults := make([]SnapshotResult, 0, len(legacyRequests))
			for _, request := range legacyRequests {
				result, err := legacy.SnapshotReader(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				legacyResults = append(legacyResults, result)
			}
			legacyElapsed := time.Since(started)
			started = time.Now()
			batchResult, err := batch.SnapshotReadersBatch(context.Background(), ReaderSnapshotBatchRequest{Items: readerBatchRequests(testCase.payloads, testCase.name+"-batch")})
			if err != nil {
				t.Fatal(err)
			}
			batchElapsed := time.Since(started)
			verifyReaderBenchmarkResults(t, legacy, testCase.payloads, legacyResults)
			batchSnapshots := make([]SnapshotResult, len(batchResult.Items))
			for index, item := range batchResult.Items {
				if item.Err != nil {
					t.Fatalf("batch item %d: %v", index, item.Err)
				}
				batchSnapshots[index] = item.Snapshot
			}
			verifyReaderBenchmarkResults(t, batch, testCase.payloads, batchSnapshots)
			t.Logf("dataset=%s files=%d bytes=%d legacy_elapsed=%s legacy_snapshots=%d legacy_packs=%d batch_elapsed=%s batch_snapshots=%d batch_packs=%d", testCase.name, len(testCase.payloads), totalPayloadBytes(testCase.payloads), legacyElapsed, len(legacyResults), countReaderRepositoryPacks(t, legacyPath), batchElapsed, len(batchSnapshots), countReaderRepositoryPacks(t, batchPath))
		})
	}
}

func initializedReaderStore(t *testing.T, repositoryPath string) *Store {
	t.Helper()
	store, err := New(Config{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: repositoryPath}}, RepositoryPassword: "repository-password"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store
}

func readerBatchRequests(payloads [][]byte, prefix string) []ReaderSnapshotRequest {
	requests := make([]ReaderSnapshotRequest, len(payloads))
	for index, payload := range payloads {
		value := append([]byte(nil), payload...)
		hash := sha256.Sum256(value)
		requests[index] = ReaderSnapshotRequest{Name: "payload", Size: int64(len(value)), ExpectedChecksum: "sha256:" + hex.EncodeToString(hash[:]), Hostname: "flashyun-reader-test", IdempotencyKey: fmt.Sprintf("%s-%d", prefix, index), Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(value)), nil
		}}
	}
	return requests
}

func deterministicPayloads(count, size int) [][]byte {
	values := make([][]byte, count)
	for index := range values {
		seed := sha256.Sum256([]byte(fmt.Sprintf("flashyun-benchmark-%d", index)))
		value := make([]byte, size)
		for offset := range value {
			value[offset] = seed[offset%len(seed)] ^ byte(offset/len(seed))
		}
		values[index] = value
	}
	return values
}

func verifyReaderBenchmarkResults(t *testing.T, store *Store, payloads [][]byte, results []SnapshotResult) {
	t.Helper()
	if len(results) != len(payloads) {
		t.Fatalf("results=%d payloads=%d", len(results), len(payloads))
	}
	for index, result := range results {
		hash := sha256.Sum256(payloads[index])
		if _, err := store.VerifySnapshotPayload(context.Background(), VerifySnapshotPayloadRequest{SnapshotID: result.ID, ExpectedSize: int64(len(payloads[index])), ExpectedChecksum: "sha256:" + hex.EncodeToString(hash[:])}); err != nil {
			t.Fatalf("verify result %d: %v", index, err)
		}
	}
}

func countReaderRepositoryPacks(t *testing.T, repositoryPath string) int {
	t.Helper()
	count := 0
	err := filepath.WalkDir(filepath.Join(repositoryPath, "data"), func(_ string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func totalPayloadBytes(payloads [][]byte) int64 {
	var total int64
	for _, payload := range payloads {
		total += int64(len(payload))
	}
	return total
}

func TestStoreSnapshotsReopenableReaderWithoutCompleteWorkFile(t *testing.T) {
	ctx := context.Background()
	payload := []byte("FlashYun provider-staged reader payload")
	repositoryPath := filepath.Join(t.TempDir(), "repository")
	store, err := New(Config{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: repositoryPath}}, RepositoryPassword: "repository-password"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	var opens atomic.Int64
	hash := sha256.Sum256(payload)
	result, err := store.SnapshotReader(ctx, ReaderSnapshotRequest{
		Name: "payload", Size: int64(len(payload)), ExpectedChecksum: "sha256:" + hex.EncodeToString(hash[:]), Hostname: "flashyun-reader-test", IdempotencyKey: "reader-workflow-1",
		Open: func(context.Context) (io.ReadCloser, error) {
			opens.Add(1)
			return io.NopCloser(bytes.NewReader(payload)), nil
		},
	})
	if err != nil {
		t.Fatalf("SnapshotReader() error = %v", err)
	}
	if result.ID == "" || result.ProcessedBytes != uint64(len(payload)) || result.Checksum != "sha256:"+hex.EncodeToString(hash[:]) {
		t.Fatalf("SnapshotReader() = %#v", result)
	}
	if opens.Load() != 1 {
		t.Fatalf("Open calls = %d, want 1", opens.Load())
	}
	restorePath := filepath.Join(t.TempDir(), "restore")
	if err := store.Restore(ctx, RestoreRequest{SnapshotID: result.ID, DestinationPath: restorePath}); err != nil {
		t.Fatal(err)
	}
	if !findFilePayload(t, restorePath, string(payload)) {
		t.Fatal("reader snapshot did not restore the payload")
	}
}

func TestStoreReusesReaderSnapshotForTheSameOperationKey(t *testing.T) {
	ctx := context.Background()
	payload := []byte("retry-safe provider payload")
	store, err := New(Config{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: filepath.Join(t.TempDir(), "repository")}}, RepositoryPassword: "repository-password"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	request := ReaderSnapshotRequest{
		Name: "payload", Size: int64(len(payload)), Hostname: "flashyun-reader-test", IdempotencyKey: "retry-case",
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil },
	}
	first, err := store.SnapshotReader(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	request.ReconcileExisting = true
	second, err := store.SnapshotReader(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || second.ID != first.ID || second.Checksum != first.Checksum {
		t.Fatalf("retry returned different snapshot: first=%#v second=%#v", first, second)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SnapshotCount != 1 {
		t.Fatalf("snapshot count = %d, want 1", stats.SnapshotCount)
	}
}

func TestStoreCreateOnlyReaderSnapshotDoesNotAdoptHistory(t *testing.T) {
	ctx := context.Background()
	payload := []byte("history-independent payload")
	store, err := New(Config{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: filepath.Join(t.TempDir(), "repository")}}, RepositoryPassword: "repository-password"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	request := ReaderSnapshotRequest{
		Name: "payload", Size: int64(len(payload)), Hostname: "flashyun-reader-test", IdempotencyKey: "create-only-case",
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil },
	}
	first, err := store.SnapshotReader(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.SnapshotReader(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == "" || second.ID == "" || first.ID == second.ID {
		t.Fatalf("create-only snapshots were unexpectedly reconciled: first=%q second=%q", first.ID, second.ID)
	}
}

func TestStoreRejectsReaderWhoseObservedBytesDoNotMatch(t *testing.T) {
	ctx := context.Background()
	store, err := New(Config{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: filepath.Join(t.TempDir(), "repository")}}, RepositoryPassword: "repository-password"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = store.SnapshotReader(ctx, ReaderSnapshotRequest{
		Name: "payload", Size: 5, Hostname: "flashyun-reader-test", IdempotencyKey: "reader-workflow-2",
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader([]byte("bad"))), nil },
	})
	if err == nil {
		t.Fatal("SnapshotReader() accepted a truncated provider object")
	}
}

func TestStoreRejectsReaderWhoseChecksumChangesAtTheSameSize(t *testing.T) {
	ctx := context.Background()
	store, err := New(Config{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: filepath.Join(t.TempDir(), "repository")}}, RepositoryPassword: "repository-password"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256([]byte("good"))
	_, err = store.SnapshotReader(ctx, ReaderSnapshotRequest{
		Name: "payload", Size: 4, ExpectedChecksum: "sha256:" + hex.EncodeToString(expected[:]), Hostname: "flashyun-reader-test", IdempotencyKey: "reader-workflow-3",
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("evil"))), nil
		},
	})
	if err == nil {
		t.Fatal("SnapshotReader() accepted provider bytes that changed without changing size")
	}
}

func TestStoreRejectsUnsafeReaderWriteConcurrency(t *testing.T) {
	ctx := context.Background()
	store, err := New(Config{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: filepath.Join(t.TempDir(), "repository")}}, RepositoryPassword: "repository-password"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.SnapshotReader(ctx, ReaderSnapshotRequest{
		Name: "payload", Size: 1, Hostname: "flashyun-reader-test", IdempotencyKey: "reader-workflow-4", WriteConcurrency: 65,
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader([]byte("x"))), nil
		},
	})
	if err == nil {
		t.Fatal("SnapshotReader() accepted write concurrency above the typed runtime bound")
	}
}

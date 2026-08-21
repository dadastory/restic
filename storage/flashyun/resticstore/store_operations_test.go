package resticstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStoreVerifySnapshotPayloadStreamsExactSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newLocalStore(t)
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	payload := []byte(strings.Repeat("stream-verified-payload", 4096))
	digest := sha256.Sum256(payload)
	expectedChecksum := "sha256:" + hex.EncodeToString(digest[:])
	snapshot, err := store.SnapshotReader(ctx, ReaderSnapshotRequest{
		Name: "payload", Size: int64(len(payload)), ExpectedChecksum: expectedChecksum,
		IdempotencyKey: "verify-streaming-snapshot", Hostname: "verify-test",
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(payload)), nil
		},
	})
	if err != nil {
		t.Fatalf("SnapshotReader() error = %v", err)
	}

	result, err := store.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{
		SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: expectedChecksum,
	})
	if err != nil {
		t.Fatalf("VerifySnapshotPayload() error = %v", err)
	}
	if result.Size != int64(len(payload)) || result.Checksum != expectedChecksum {
		t.Fatalf("VerifySnapshotPayload() = %#v", result)
	}

	_, err = store.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{
		SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)) + 1, ExpectedChecksum: expectedChecksum,
	})
	if err == nil {
		t.Fatal("VerifySnapshotPayload(size mismatch) error = nil")
	}
	_, err = store.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{
		SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: "sha256:" + strings.Repeat("0", 64),
	})
	if err == nil {
		t.Fatal("VerifySnapshotPayload(checksum mismatch) error = nil")
	}
}

func TestStoreVerifySnapshotPayloadRejectsMissingPayloadAndCancellation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newLocalStore(t)
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	snapshot, err := store.Snapshot(ctx, SnapshotRequest{
		SourcePath: writeStagedFile(t, "not-payload", "data"), Hostname: "verify-test",
	})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if _, err := store.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{SnapshotID: snapshot.ID, ExpectedSize: 4}); err == nil {
		t.Fatal("VerifySnapshotPayload(missing payload) error = nil")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.VerifySnapshotPayload(cancelled, VerifySnapshotPayloadRequest{SnapshotID: snapshot.ID, ExpectedSize: 4}); !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifySnapshotPayload(cancelled) error = %v, want context canceled", err)
	}
}

func TestStoreCopyVerifyBatchLoadsRepositoriesOnceAndKeepsPerItemProof(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := newLocalStore(t)
	destination := newLocalStore(t)
	for _, store := range []*Store{source, destination} {
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize() error = %v", err)
		}
	}

	var observed []string
	observe := func(event string) { observed = append(observed, event) }
	source.observeCopySession = observe
	destination.observeCopySession = observe
	items := make([]CopyVerifyItem, 0, 2)
	for index, value := range []string{"batch payload one", "batch payload two"} {
		payload := []byte(value)
		digest := sha256.Sum256(payload)
		checksum := "sha256:" + hex.EncodeToString(digest[:])
		snapshot, err := source.SnapshotReader(ctx, ReaderSnapshotRequest{
			Name: "payload", Size: int64(len(payload)), ExpectedChecksum: checksum,
			IdempotencyKey: fmt.Sprintf("copy-verify-batch-%d", index), Hostname: "copy-verify-batch-test",
			Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil },
		})
		if err != nil {
			t.Fatalf("SnapshotReader(%d) error = %v", index, err)
		}
		items = append(items, CopyVerifyItem{SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: checksum})
	}

	result, err := source.CopyAndVerifyBatch(ctx, destination, CopyVerifyBatchRequest{Items: items, CreateOnly: true})
	if err != nil {
		t.Fatalf("CopyAndVerifyBatch() error = %v", err)
	}
	if len(result.Items) != len(items) {
		t.Fatalf("CopyAndVerifyBatch() items = %d, want %d", len(result.Items), len(items))
	}
	for index, item := range result.Items {
		if item.Err != nil || item.Mapping.SourceSnapshotID != items[index].SnapshotID || item.Mapping.ReplacementSnapshotID == "" ||
			item.Verification.Size != items[index].ExpectedSize || item.Verification.Checksum != items[index].ExpectedChecksum {
			t.Fatalf("CopyAndVerifyBatch() item %d = %#v", index, item)
		}
	}
	if strings.Join(observed, ",") != "source_index_loaded,target_index_loaded" {
		t.Fatalf("copy session events = %v", observed)
	}
}

func TestStoreCopyVerifyBatchRetainsMappingWhenOnePayloadProofFails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := newLocalStore(t)
	destination := newLocalStore(t)
	for _, store := range []*Store{source, destination} {
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize() error = %v", err)
		}
	}
	payload := []byte("batch verification failure")
	digest := sha256.Sum256(payload)
	checksum := "sha256:" + hex.EncodeToString(digest[:])
	snapshot, err := source.SnapshotReader(ctx, ReaderSnapshotRequest{
		Name: "payload", Size: int64(len(payload)), ExpectedChecksum: checksum,
		IdempotencyKey: "copy-verify-batch-failure", Hostname: "copy-verify-batch-test",
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.CopyAndVerifyBatch(ctx, destination, CopyVerifyBatchRequest{CreateOnly: true, Items: []CopyVerifyItem{
		{SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: "sha256:" + strings.Repeat("0", 64)},
	}})
	if err != nil {
		t.Fatalf("CopyAndVerifyBatch() session error = %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].Err == nil || result.Items[0].Mapping.ReplacementSnapshotID == "" {
		t.Fatalf("CopyAndVerifyBatch() result = %#v", result)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := source.CopyAndVerifyBatch(cancelled, destination, CopyVerifyBatchRequest{CreateOnly: true, Items: []CopyVerifyItem{{SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: checksum}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("CopyAndVerifyBatch(cancelled) error = %v", err)
	}
}

func TestStoreCopyVerifyBatchReconcilesOriginalMappingsOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := newLocalStore(t)
	destination := newLocalStore(t)
	for _, store := range []*Store{source, destination} {
		if err := store.Initialize(ctx); err != nil {
			t.Fatal(err)
		}
	}
	payload := []byte("reconcile batch payload")
	digest := sha256.Sum256(payload)
	checksum := "sha256:" + hex.EncodeToString(digest[:])
	snapshot, err := source.SnapshotReader(ctx, ReaderSnapshotRequest{
		Name: "payload", Size: int64(len(payload)), ExpectedChecksum: checksum,
		IdempotencyKey: "copy-verify-batch-reconcile", Hostname: "copy-verify-batch-test",
		Open: func(context.Context) (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload)), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := source.CopyAndVerifyBatch(ctx, destination, CopyVerifyBatchRequest{CreateOnly: true, Items: []CopyVerifyItem{{SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: checksum}}})
	if err != nil || len(created.Items) != 1 || created.Items[0].Err != nil {
		t.Fatalf("initial CopyAndVerifyBatch() = %#v, %v", created, err)
	}
	var observed []string
	destination.observeCopySession = func(event string) { observed = append(observed, event) }
	reconciled, err := source.CopyAndVerifyBatch(ctx, destination, CopyVerifyBatchRequest{Items: []CopyVerifyItem{
		{SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: checksum},
		{SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: checksum},
	}})
	if err == nil {
		t.Fatal("duplicate source snapshot IDs must be rejected")
	}
	if len(reconciled.Items) != 0 {
		t.Fatalf("invalid batch returned items: %#v", reconciled.Items)
	}
	result, err := source.CopyAndVerifyBatch(ctx, destination, CopyVerifyBatchRequest{Items: []CopyVerifyItem{{SnapshotID: snapshot.ID, ExpectedSize: int64(len(payload)), ExpectedChecksum: checksum}}})
	if err != nil || len(result.Items) != 1 || result.Items[0].Err != nil || result.Items[0].Mapping != created.Items[0].Mapping {
		t.Fatalf("reconcile CopyAndVerifyBatch() = %#v, %v", result, err)
	}
	count := 0
	for _, event := range observed {
		if event == "destination_originals_loaded" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("destination Original maps loaded %d times, want 1; events=%v", count, observed)
	}
}

func TestStoreCopiesSnapshotsBetweenIndependentLocalRepositories(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := newLocalStore(t)
	destination := newLocalStore(t)
	for _, store := range []*Store{source, destination} {
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize() error = %v", err)
		}
	}

	staged := writeStagedFile(t, "copy.txt", "copy payload")
	snapshot, err := source.Snapshot(ctx, SnapshotRequest{SourcePath: staged, Hostname: "copy-test"})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	result, err := source.Copy(ctx, destination, CopyRequest{SnapshotIDs: []string{snapshot.ID}})
	if err != nil {
		t.Fatalf("Copy() error = %v", err)
	}
	if result.CopiedSnapshots != 1 || len(result.SnapshotIDs) != 1 || result.SnapshotIDs[0] == "" {
		t.Fatalf("Copy() result = %#v", result)
	}
	if len(result.Mappings) != 1 || result.Mappings[0].SourceSnapshotID != snapshot.ID || result.Mappings[0].ReplacementSnapshotID != result.SnapshotIDs[0] {
		t.Fatalf("Copy() mappings = %#v", result.Mappings)
	}
	repeated, err := source.Copy(ctx, destination, CopyRequest{SnapshotIDs: []string{snapshot.ID}})
	if err != nil {
		t.Fatalf("repeat Copy() error = %v", err)
	}
	if repeated.CopiedSnapshots != 0 || repeated.SkippedSnapshots != 1 || len(repeated.Mappings) != 1 || repeated.Mappings[0] != result.Mappings[0] {
		t.Fatalf("repeat Copy() result = %#v, want reusable mapping %#v", repeated, result.Mappings[0])
	}

	restored := filepath.Join(t.TempDir(), "restore")
	if err := destination.Restore(ctx, RestoreRequest{SnapshotID: result.SnapshotIDs[0], DestinationPath: restored}); err != nil {
		t.Fatalf("destination Restore() error = %v", err)
	}
	if !findFilePayload(t, restored, "copy payload") {
		t.Fatal("copied snapshot did not restore payload")
	}
}

func TestStoreCreateOnlyCopySkipsUnrelatedDestinationSnapshotHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := newLocalStore(t)
	destination := newLocalStore(t)
	for _, store := range []*Store{source, destination} {
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize() error = %v", err)
		}
	}

	sourceSnapshot, err := source.Snapshot(ctx, SnapshotRequest{
		SourcePath: writeStagedFile(t, "create-only-source.txt", "create-only payload"),
		Hostname:   "create-only-copy-test",
	})
	if err != nil {
		t.Fatalf("source Snapshot() error = %v", err)
	}
	unrelated, err := destination.Snapshot(ctx, SnapshotRequest{
		SourcePath: writeStagedFile(t, "unrelated-history.txt", "unrelated history"),
		Hostname:   "create-only-copy-test",
	})
	if err != nil {
		t.Fatalf("destination Snapshot() error = %v", err)
	}
	root := destination.config.Provider.Repository.Root
	unrelatedMetadata := filepath.Join(root, "snapshots", unrelated.ID)
	if err := os.Chmod(unrelatedMetadata, 0o600); err != nil {
		t.Fatalf("make unrelated snapshot metadata writable: %v", err)
	}
	if err := os.WriteFile(unrelatedMetadata, []byte("invalid encrypted snapshot metadata"), 0o600); err != nil {
		t.Fatalf("corrupt unrelated snapshot metadata: %v", err)
	}

	result, err := source.Copy(ctx, destination, CopyRequest{
		SnapshotIDs: []string{sourceSnapshot.ID},
		CreateOnly:  true,
	})
	if err != nil {
		t.Fatalf("create-only Copy() scanned unrelated history: %v", err)
	}
	if result.CopiedSnapshots != 1 || result.SkippedSnapshots != 0 || len(result.Mappings) != 1 ||
		result.Mappings[0].SourceSnapshotID != sourceSnapshot.ID || result.Mappings[0].ReplacementSnapshotID == "" {
		t.Fatalf("create-only Copy() result = %#v", result)
	}
}

func TestStoreCopyReusesOfficialMetadataCacheWithoutCachingDataPacks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cacheRoot := filepath.Join(t.TempDir(), "restic-cache")
	sourceRoot := filepath.Join(t.TempDir(), "source-repository")
	targetRoot := filepath.Join(t.TempDir(), "target-repository")
	newCachedStore := func(root string) *Store {
		store, err := New(Config{
			Provider:           testLocalProvider(root),
			RepositoryPassword: "repository-password",
			CacheDirectory:     cacheRoot,
		})
		if err != nil {
			t.Fatalf("New(cached store) error = %v", err)
		}
		return store
	}
	source := newCachedStore(sourceRoot)
	target := newCachedStore(targetRoot)
	for _, store := range []*Store{source, target} {
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize() error = %v", err)
		}
	}
	snapshot, err := source.Snapshot(ctx, SnapshotRequest{
		SourcePath: writeStagedFile(t, "cache-payload.bin", strings.Repeat("payload", 64*1024)),
		Hostname:   "metadata-cache-test",
	})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	dataPackID := filepath.Base(repositoryDataPack(t, ctx, source, sourceRoot))
	if _, err := source.Copy(ctx, target, CopyRequest{SnapshotIDs: []string{snapshot.ID}, CreateOnly: true}); err != nil {
		t.Fatalf("Copy() error = %v", err)
	}

	sourceRepository, err := source.openRepository(ctx)
	if err != nil {
		t.Fatalf("openRepository() error = %v", err)
	}
	sourceRepositoryID := sourceRepository.Config().ID
	_ = sourceRepository.Close()
	dataCachePath := filepath.Join(cacheRoot, sourceRepositoryID, "data", dataPackID[:2], dataPackID)
	if _, err := os.Stat(dataCachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ordinary data pack was retained in metadata cache: %v", err)
	}
	indexFiles := 0
	if err := filepath.WalkDir(cacheRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && filepath.Base(filepath.Dir(filepath.Dir(path))) == "index" {
			indexFiles++
		}
		return nil
	}); err != nil {
		t.Fatalf("walk metadata cache: %v", err)
	}
	if indexFiles == 0 {
		t.Fatal("Restic index objects were not retained in metadata cache")
	}
}

func TestCleanupCacheRemovesOnlyOldRepositoryIDDirectories(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	oldRepository := filepath.Join(root, strings.Repeat("a", 64))
	freshRepository := filepath.Join(root, strings.Repeat("b", 64))
	unrelated := filepath.Join(root, "operator-data")
	for _, directory := range []string{oldRepository, freshRepository, unrelated} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(oldRepository, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(unrelated, old, old); err != nil {
		t.Fatal(err)
	}
	if err := CleanupCache(root, time.Hour); err != nil {
		t.Fatalf("CleanupCache() error = %v", err)
	}
	if _, err := os.Stat(oldRepository); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old repository cache still exists: %v", err)
	}
	for _, directory := range []string{freshRepository, unrelated} {
		if _, err := os.Stat(directory); err != nil {
			t.Fatalf("safe directory %q was removed: %v", filepath.Base(directory), err)
		}
	}
}

func TestStoreCopyMappingNamesTheDirectSourceAcrossReplicaHops(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := newLocalStore(t)
	firstReplica := newLocalStore(t)
	secondReplica := newLocalStore(t)
	for _, store := range []*Store{source, firstReplica, secondReplica} {
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize() error = %v", err)
		}
	}

	snapshot, err := source.Snapshot(ctx, SnapshotRequest{
		SourcePath: writeStagedFile(t, "replica-hop.txt", "replica hop payload"),
		Hostname:   "replica-hop-test",
	})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	first, err := source.Copy(ctx, firstReplica, CopyRequest{SnapshotIDs: []string{snapshot.ID}})
	if err != nil {
		t.Fatalf("first Copy() error = %v", err)
	}
	if len(first.Mappings) != 1 {
		t.Fatalf("first Copy() mappings = %#v", first.Mappings)
	}

	directSourceID := first.Mappings[0].ReplacementSnapshotID
	second, err := firstReplica.Copy(ctx, secondReplica, CopyRequest{SnapshotIDs: []string{directSourceID}})
	if err != nil {
		t.Fatalf("second Copy() error = %v", err)
	}
	if len(second.Mappings) != 1 {
		t.Fatalf("second Copy() mappings = %#v", second.Mappings)
	}
	if second.Mappings[0].SourceSnapshotID != directSourceID {
		t.Fatalf("second Copy() source = %q, want direct source %q", second.Mappings[0].SourceSnapshotID, directSourceID)
	}
	if second.Mappings[0].ReplacementSnapshotID == "" {
		t.Fatal("second Copy() returned an empty replacement snapshot ID")
	}
}

func TestStoreSerializesConcurrentCopiesIntoOneDestinationRepository(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := newLocalStore(t)
	destination := newLocalStore(t)
	for _, store := range []*Store{source, destination} {
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize() error = %v", err)
		}
	}

	const copies = 6
	snapshotIDs := make([]string, 0, copies)
	for index := 0; index < copies; index++ {
		snapshot, err := source.Snapshot(ctx, SnapshotRequest{
			SourcePath: writeStagedFile(t, "concurrent-copy.txt", strings.Repeat(string(rune('a'+index)), 256*1024)),
			Hostname:   "concurrent-copy-test",
		})
		if err != nil {
			t.Fatalf("Snapshot(%d) error = %v", index, err)
		}
		snapshotIDs = append(snapshotIDs, snapshot.ID)
	}

	start := make(chan struct{})
	errorsByCopy := make([]error, copies)
	results := make([]CopyResult, copies)
	var group sync.WaitGroup
	for index, snapshotID := range snapshotIDs {
		group.Add(1)
		go func(index int, snapshotID string) {
			defer group.Done()
			<-start
			results[index], errorsByCopy[index] = source.Copy(ctx, destination, CopyRequest{SnapshotIDs: []string{snapshotID}})
		}(index, snapshotID)
	}
	close(start)
	group.Wait()

	for index, copyErr := range errorsByCopy {
		if copyErr != nil {
			t.Fatalf("concurrent Copy(%d) error = %v", index, copyErr)
		}
		if len(results[index].Mappings) != 1 || results[index].Mappings[0].SourceSnapshotID != snapshotIDs[index] {
			t.Fatalf("concurrent Copy(%d) mappings = %#v", index, results[index].Mappings)
		}
	}
	if err := destination.VerifySnapshots(ctx, snapshotIDsFromMappings(results)); err != nil {
		t.Fatalf("VerifySnapshots() after concurrent copies = %v", err)
	}
}

func snapshotIDsFromMappings(results []CopyResult) []string {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		if len(result.Mappings) == 1 {
			ids = append(ids, result.Mappings[0].ReplacementSnapshotID)
		}
	}
	return ids
}

func TestStoreForgetPruneRetainsLatestSnapshots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newLocalStore(t)
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	for index := 0; index < 3; index++ {
		staged := writeStagedFile(t, "version.txt", string(rune('a'+index)))
		_, err := store.Snapshot(ctx, SnapshotRequest{
			SourcePath:   staged,
			Hostname:     "retention-test",
			SnapshotTime: time.Date(2026, time.January, index+1, 0, 0, 0, 0, time.UTC),
		})
		if err != nil {
			t.Fatalf("Snapshot(%d) error = %v", index, err)
		}
	}

	result, err := store.ForgetPrune(ctx, RetentionRequest{KeepLast: 1})
	if err != nil {
		t.Fatalf("ForgetPrune() error = %v", err)
	}
	if result.ForgottenSnapshots != 2 || result.KeptSnapshots != 1 {
		t.Fatalf("ForgetPrune() result = %#v", result)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.SnapshotCount != 1 {
		t.Fatalf("SnapshotCount = %d, want 1", stats.SnapshotCount)
	}
}

func TestStoreForgetPruneKeepsProtectedHistoricalSnapshots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newLocalStore(t)
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	ids := make([]string, 0, 3)
	for index := 0; index < 3; index++ {
		staged := writeStagedFile(t, "protected.txt", string(rune('a'+index)))
		snapshot, err := store.Snapshot(ctx, SnapshotRequest{SourcePath: staged, Hostname: "retention-protected-test", SnapshotTime: time.Date(2026, time.February, index+1, 0, 0, 0, 0, time.UTC)})
		if err != nil {
			t.Fatalf("Snapshot(%d) error = %v", index, err)
		}
		ids = append(ids, snapshot.ID)
	}

	result, err := store.ForgetPrune(ctx, RetentionRequest{KeepLast: 1, ProtectedSnapshotIDs: []string{ids[0]}})
	if err != nil {
		t.Fatalf("ForgetPrune() error = %v", err)
	}
	if result.ForgottenSnapshots != 1 || result.KeptSnapshots != 2 {
		t.Fatalf("ForgetPrune() result = %#v", result)
	}
	restored := filepath.Join(t.TempDir(), "protected-restore")
	if err := store.Restore(ctx, RestoreRequest{SnapshotID: ids[0], DestinationPath: restored}); err != nil {
		t.Fatalf("Restore(protected) error = %v", err)
	}
}

func TestStoreForgetPruneForgetsOnlyExplicitSnapshots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newLocalStore(t)
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	ids := make([]string, 0, 3)
	for index := 0; index < 3; index++ {
		staged := writeStagedFile(t, "explicit.txt", string(rune('a'+index)))
		snapshot, err := store.Snapshot(ctx, SnapshotRequest{
			SourcePath:   staged,
			Hostname:     "retention-explicit-test",
			SnapshotTime: time.Date(2026, time.March, index+1, 0, 0, 0, 0, time.UTC),
		})
		if err != nil {
			t.Fatalf("Snapshot(%d) error = %v", index, err)
		}
		ids = append(ids, snapshot.ID)
	}

	result, err := store.ForgetPrune(ctx, RetentionRequest{
		ForgetSnapshotIDs:    []string{ids[1]},
		ProtectedSnapshotIDs: []string{ids[0], ids[2]},
	})
	if err != nil {
		t.Fatalf("ForgetPrune() error = %v", err)
	}
	if result.ForgottenSnapshots != 1 || result.KeptSnapshots != 2 {
		t.Fatalf("ForgetPrune() result = %#v", result)
	}
	for _, id := range []string{ids[0], ids[2]} {
		restored := filepath.Join(t.TempDir(), "explicit-retained-restore")
		if err := store.Restore(ctx, RestoreRequest{SnapshotID: id, DestinationPath: restored}); err != nil {
			t.Fatalf("Restore(%s) error = %v", id, err)
		}
	}
	if err := store.Restore(ctx, RestoreRequest{SnapshotID: ids[1], DestinationPath: filepath.Join(t.TempDir(), "explicit-forgotten-restore")}); err == nil {
		t.Fatal("Restore(forgotten) error = nil")
	}
}

func TestStoreForgetPruneResumesWhenExactSnapshotWasAlreadyRemoved(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newLocalStore(t)
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	snapshot, err := store.Snapshot(ctx, SnapshotRequest{SourcePath: writeStagedFile(t, "resume.txt", "payload"), Hostname: "retention-resume-test"})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	request := RetentionRequest{ForgetSnapshotIDs: []string{snapshot.ID}}
	if _, err := store.ForgetPrune(ctx, request); err != nil {
		t.Fatalf("first ForgetPrune() error = %v", err)
	}
	result, err := store.ForgetPrune(ctx, request)
	if err != nil {
		t.Fatalf("resumed ForgetPrune() error = %v", err)
	}
	if result.ForgottenSnapshots != 1 || result.KeptSnapshots != 0 {
		t.Fatalf("resumed ForgetPrune() result = %#v", result)
	}
}

func TestStoreVerifySnapshotsRejectsMissingSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newLocalStore(t)
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	snapshot, err := store.Snapshot(ctx, SnapshotRequest{SourcePath: writeStagedFile(t, "verify.txt", "payload"), Hostname: "verify-test"})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if err := store.VerifySnapshots(ctx, []string{snapshot.ID}); err != nil {
		t.Fatalf("VerifySnapshots(existing) error = %v", err)
	}
	if err := store.VerifySnapshots(ctx, []string{"0000000000000000000000000000000000000000000000000000000000000000"}); err == nil {
		t.Fatal("VerifySnapshots(missing) error = nil")
	}
}

func TestStoreOperationsRespectCanceledContextAndRedactSecrets(t *testing.T) {
	t.Parallel()
	store, err := New(Config{
		Provider:           testS3Provider("127.0.0.1:1", true, "flashyun", "", "", "access-secret", "secret-key"),
		RepositoryPassword: "repository-secret",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Copy(canceled, store, CopyRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Copy(canceled) error = %v, want context canceled", err)
	}
	if _, err := store.ForgetPrune(canceled, RetentionRequest{KeepLast: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ForgetPrune(canceled) error = %v, want context canceled", err)
	}
	operationContext, stopOperation := context.WithTimeout(context.Background(), time.Second)
	defer stopOperation()
	_, err = store.Stats(operationContext)
	if err == nil {
		t.Fatal("Stats() error = nil")
	}
	for _, secret := range []string{"access-secret", "secret-key", "repository-secret"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("operation error leaked %q: %v", secret, err)
		}
	}
}

func TestRepositoryInitializationPermitRespectsCancellation(t *testing.T) {
	permit := make(chan struct{}, 1)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := acquireInitializationPermit(canceled, permit); !errors.Is(err, context.Canceled) {
		t.Fatalf("acquireInitializationPermit(canceled) error = %v, want context canceled", err)
	}
}

func TestCopyErrorRedactsSourceAndDestinationSecrets(t *testing.T) {
	source := newSecretStore(t, "source-access", "source-secret", "source-password")
	destination := newSecretStore(t, "destination-access", "destination-secret", "destination-password")
	err := operationErrorForStores("copy snapshot", errors.New("source-access source-secret source-password destination-access destination-secret destination-password"), source, destination)
	for _, secret := range []string{"source-access", "source-secret", "source-password", "destination-access", "destination-secret", "destination-password"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("copy error leaked %q: %v", secret, err)
		}
	}
}

func newLocalStore(t *testing.T) *Store {
	t.Helper()
	store, err := New(Config{
		Provider:           testLocalProvider(filepath.Join(t.TempDir(), "repo")),
		RepositoryPassword: "repository-password",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store
}

func writeStagedFile(t *testing.T, name, contents string) string {
	t.Helper()
	staged := filepath.Join(t.TempDir(), "staged")
	if err := os.MkdirAll(staged, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staged, name), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return staged
}

func newSecretStore(t *testing.T, accessKey, secretKey, password string) *Store {
	t.Helper()
	store, err := New(Config{
		Provider:           testS3Provider("127.0.0.1:1", true, "flashyun", "", "", accessKey, secretKey),
		RepositoryPassword: password,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store
}

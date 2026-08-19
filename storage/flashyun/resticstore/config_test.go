package resticstore

import (
	"context"
	"crypto/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/restic/restic/internal/restic"
)

func TestConfigValidateAcceptsTypedLocalAndS3Providers(t *testing.T) {
	t.Parallel()

	for _, config := range []Config{
		{
			Provider:           Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: t.TempDir()}},
			RepositoryPassword: "repository-password",
		},
		{
			Provider: Provider{Kind: ProviderS3, S3: &S3Provider{
				Endpoint:  "127.0.0.1:9000",
				UseHTTP:   true,
				Bucket:    "flashyun",
				Prefix:    "workspaces/workspace-1",
				AccessKey: "access-key",
				SecretKey: "secret-key",
				Transport: http.DefaultTransport,
			}},
			RepositoryPassword: "repository-password",
		},
	} {
		if err := config.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	}
}

func TestConfigValidateAcceptsOnlyPrivateAbsoluteCacheRoot(t *testing.T) {
	t.Parallel()
	provider := Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: filepath.Join(t.TempDir(), "repository")}}
	valid := Config{Provider: provider, RepositoryPassword: "repository-password", CacheDirectory: filepath.Join(t.TempDir(), "restic-cache")}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate(absolute cache) error = %v", err)
	}
	for _, cacheDirectory := range []string{"relative-cache", string(filepath.Separator)} {
		invalid := valid
		invalid.CacheDirectory = cacheDirectory
		if err := invalid.Validate(); err == nil {
			t.Fatalf("Validate(cache=%q) error = nil", cacheDirectory)
		}
	}
}

func TestConfigValidateRejectsIncompleteOrAmbiguousProvider(t *testing.T) {
	t.Parallel()

	for _, config := range []Config{
		{RepositoryPassword: "repository-password"},
		{Provider: Provider{Kind: ProviderLocal}, RepositoryPassword: "repository-password"},
		{Provider: Provider{Kind: ProviderS3, S3: &S3Provider{Endpoint: "https://s3.example.test", Bucket: "bucket", AccessKey: "a", SecretKey: "b"}}, RepositoryPassword: "repository-password"},
		{Provider: Provider{Kind: ProviderS3, S3: &S3Provider{Endpoint: "s3.example.test", Bucket: "bucket", AccessKey: "a"}}, RepositoryPassword: "repository-password"},
		{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: "relative/path"}}, RepositoryPassword: "repository-password"},
		{Provider: Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: t.TempDir()}}, RepositoryPassword: ""},
	} {
		if err := config.Validate(); err == nil {
			t.Fatalf("Validate() error = nil for %#v", config.Provider)
		}
	}
}

func TestConfigRedactedDoesNotExposeSecrets(t *testing.T) {
	t.Parallel()

	config := Config{
		Provider: Provider{Kind: ProviderS3, S3: &S3Provider{
			Endpoint:  "127.0.0.1:9000",
			UseHTTP:   true,
			Bucket:    "flashyun",
			Prefix:    "workspaces/workspace-1",
			AccessKey: "access-key-value",
			SecretKey: "secret-key-value",
		}},
		RepositoryPassword: "repository-password-value",
	}

	redacted := config.Redacted()
	if redacted.Kind != ProviderS3 || redacted.Endpoint != "127.0.0.1:9000" || redacted.Bucket != "flashyun" {
		t.Fatalf("Redacted() = %#v", redacted)
	}
	serialized := redacted.String()
	for _, secret := range []string{"access-key-value", "secret-key-value", "repository-password-value"} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("redacted config exposed %q: %s", secret, serialized)
		}
	}
}

func TestStoreInitializesAndSnapshotsAStagedDirectory(t *testing.T) {
	t.Parallel()

	context := context.Background()
	repositoryPath := filepath.Join(t.TempDir(), "repository")
	sourcePath := filepath.Join(t.TempDir(), "staged")
	if err := os.MkdirAll(sourcePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "invoice.txt"), []byte("FlashYun snapshot payload"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := New(Config{
		Provider:           Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: repositoryPath}},
		RepositoryPassword: "repository-password",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := store.Initialize(context); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	snapshot, err := store.Snapshot(context, SnapshotRequest{SourcePath: sourcePath, Hostname: "flashyun-test"})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if snapshot.ID == "" {
		t.Fatal("Snapshot() returned an empty ID")
	}
	if snapshot.ProcessedBytes != uint64(len("FlashYun snapshot payload")) {
		t.Fatalf("Snapshot() processed bytes = %d", snapshot.ProcessedBytes)
	}

	restorePath := filepath.Join(t.TempDir(), "restore")
	if err := store.Restore(context, RestoreRequest{SnapshotID: snapshot.ID, DestinationPath: restorePath}); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if restored := findFilePayload(t, restorePath, "FlashYun snapshot payload"); !restored {
		t.Fatal("Restore() did not recover the snapshot payload")
	}

	stats, err := store.Stats(context)
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.SnapshotCount != 1 || stats.RepositoryBytes == 0 {
		t.Fatalf("Stats() = %#v", stats)
	}
	if err := store.Check(context); err != nil {
		t.Fatalf("Check() error = %v", err)
	}
}

func TestStoreCheckReadsAndRejectsCorruptPackData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repositoryPath := filepath.Join(t.TempDir(), "repository")
	sourcePath := filepath.Join(t.TempDir(), "staged")
	if err := os.MkdirAll(sourcePath, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 512*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("generate incompressible corruption fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "payload.bin"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := New(Config{
		Provider:           Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: repositoryPath}},
		RepositoryPassword: "repository-password",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if _, err := store.Snapshot(ctx, SnapshotRequest{SourcePath: sourcePath, Hostname: "flashyun-corruption-test"}); err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	packPath := repositoryDataPack(t, ctx, store, repositoryPath)
	corruptRepositoryPack(t, packPath)

	if err := store.Check(ctx); err == nil {
		t.Fatal("Check() accepted physically corrupted pack data")
	} else if failure, ok := err.(*IntegrityError); !ok || failure.IntegrityCode() != IntegrityCodeCorrupt || !failure.CorruptionVerified() || failure.IntegrityRetryable() {
		t.Fatalf("Check() corruption classification = %#v (%T)", err, err)
	}
}

func TestStoreCheckWaitsForAnActiveRepositoryReader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	repositoryPath := filepath.Join(t.TempDir(), "repository")
	store, err := New(Config{
		Provider:           Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: repositoryPath}},
		RepositoryPassword: "repository-password",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := store.Initialize(ctx); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	repository, _, unlock, err := store.openLockedRepository(ctx, false)
	if err != nil {
		t.Fatalf("open shared repository lock: %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- store.Check(ctx) }()
	select {
	case checkErr := <-result:
		unlock()
		_ = repository.Close()
		t.Fatalf("Check() returned while a concurrent repository operation still held a shared lock: %v", checkErr)
	case <-time.After(time.Second):
	}

	unlock()
	_ = repository.Close()
	if checkErr := <-result; checkErr != nil {
		t.Fatalf("Check() after concurrent repository operation completed: %v", checkErr)
	}
}

func TestStoreRepairsCorruptPackFromHealthyCopy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	payload := make([]byte, 512*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("generate repair fixture: %v", err)
	}
	sourcePath := filepath.Join(t.TempDir(), "staged")
	if err := os.MkdirAll(sourcePath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourcePath, "payload.bin"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	sourceRepositoryPath := filepath.Join(t.TempDir(), "source-repository")
	targetRepositoryPath := filepath.Join(t.TempDir(), "target-repository")
	newLocalStore := func(path string) *Store {
		store, err := New(Config{
			Provider:           Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: path}},
			RepositoryPassword: "repository-password",
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize() error = %v", err)
		}
		return store
	}
	source := newLocalStore(sourceRepositoryPath)
	target := newLocalStore(targetRepositoryPath)
	snapshot, err := source.Snapshot(ctx, SnapshotRequest{SourcePath: sourcePath, Hostname: "flashyun-repair-source"})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	initialCopy, err := source.Copy(ctx, target, CopyRequest{SnapshotIDs: []string{snapshot.ID}})
	if err != nil || len(initialCopy.Mappings) != 1 {
		t.Fatalf("initial Copy() = %#v, %v", initialCopy, err)
	}
	targetSnapshotID := initialCopy.Mappings[0].ReplacementSnapshotID
	corruptRepositoryPack(t, repositoryDataPack(t, ctx, target, targetRepositoryPath))
	if err := target.Check(ctx); err == nil {
		t.Fatal("corruption fixture did not damage target repository")
	}

	if err := target.RepairCorruptPacks(ctx); err != nil {
		t.Fatalf("RepairCorruptPacks() error = %v", err)
	}
	repairCopy, err := source.Copy(ctx, target, CopyRequest{SnapshotIDs: []string{snapshot.ID}})
	if err != nil {
		t.Fatalf("repair Copy() error = %v", err)
	}
	if repairCopy.SkippedSnapshots != 1 {
		t.Fatalf("repair Copy() must reuse immutable snapshot metadata: %#v", repairCopy)
	}
	if err := target.Check(ctx); err != nil {
		t.Fatalf("Check() after repair error = %v", err)
	}
	restorePath := filepath.Join(t.TempDir(), "restore")
	if err := target.Restore(ctx, RestoreRequest{SnapshotID: targetSnapshotID, DestinationPath: restorePath}); err != nil {
		t.Fatalf("Restore() after repair error = %v", err)
	}
	if !findFileBytes(t, restorePath, payload) {
		t.Fatal("repaired repository did not restore the original payload")
	}
}

func TestStoreRepairsTwoCorruptCopiesAndFailsClosedWhenAllCopiesAreCorrupt(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	payload := make([]byte, 768*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("generate multi-corruption fixture: %v", err)
	}
	staged := filepath.Join(t.TempDir(), "staged")
	if err := os.MkdirAll(staged, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staged, "payload.bin"), payload, 0o600); err != nil {
		t.Fatal(err)
	}

	type repositoryFixture struct {
		root       string
		store      *Store
		snapshotID string
	}
	newRepository := func(name string) repositoryFixture {
		root := filepath.Join(t.TempDir(), name)
		store, err := New(Config{
			Provider:           Provider{Kind: ProviderLocal, Local: &LocalProvider{Path: root}},
			RepositoryPassword: "repository-password",
		})
		if err != nil {
			t.Fatalf("New(%s) error = %v", name, err)
		}
		if err := store.Initialize(ctx); err != nil {
			t.Fatalf("Initialize(%s) error = %v", name, err)
		}
		return repositoryFixture{root: root, store: store}
	}

	healthy := newRepository("healthy")
	firstDamaged := newRepository("damaged-a")
	secondDamaged := newRepository("damaged-b")
	snapshot, err := healthy.store.Snapshot(ctx, SnapshotRequest{SourcePath: staged, Hostname: "flashyun-multi-corruption-source"})
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	healthy.snapshotID = snapshot.ID
	for _, target := range []*repositoryFixture{&firstDamaged, &secondDamaged} {
		copied, copyErr := healthy.store.Copy(ctx, target.store, CopyRequest{SnapshotIDs: []string{healthy.snapshotID}})
		if copyErr != nil || len(copied.Mappings) != 1 {
			t.Fatalf("initial Copy(%s) = %#v, %v", target.root, copied, copyErr)
		}
		target.snapshotID = copied.Mappings[0].ReplacementSnapshotID
		corruptRepositoryPack(t, repositoryDataPack(t, ctx, target.store, target.root))
		assertCorruptIntegrityError(t, target.store.Check(ctx))
	}

	for _, target := range []*repositoryFixture{&firstDamaged, &secondDamaged} {
		if err := target.store.RepairCorruptPacks(ctx); err != nil {
			t.Fatalf("RepairCorruptPacks(%s) error = %v", target.root, err)
		}
		copied, copyErr := healthy.store.Copy(ctx, target.store, CopyRequest{SnapshotIDs: []string{healthy.snapshotID}})
		if copyErr != nil {
			t.Fatalf("repair Copy(%s) error = %v", target.root, copyErr)
		}
		if copied.SkippedSnapshots != 1 {
			t.Fatalf("repair Copy(%s) must preserve immutable snapshot identity: %#v", target.root, copied)
		}
		if err := target.store.Check(ctx); err != nil {
			t.Fatalf("Check(%s) after repair error = %v", target.root, err)
		}
		restoreRoot := filepath.Join(t.TempDir(), "restore")
		if err := target.store.Restore(ctx, RestoreRequest{SnapshotID: target.snapshotID, DestinationPath: restoreRoot}); err != nil {
			t.Fatalf("Restore(%s) after repair error = %v", target.root, err)
		}
		if !findFileBytes(t, restoreRoot, payload) {
			t.Fatalf("repaired repository %s did not restore the immutable payload", target.root)
		}
	}

	// Destroy every exact copy. Each candidate must fail its typed deep check,
	// and trying each candidate once must not yield a guessed snapshot mapping.
	allCorrupt := []*repositoryFixture{&healthy, &firstDamaged, &secondDamaged}
	for _, candidate := range allCorrupt {
		corruptRepositoryPack(t, repositoryDataPack(t, ctx, candidate.store, candidate.root))
		assertCorruptIntegrityError(t, candidate.store.Check(ctx))
	}
	recovery := newRepository("untrusted-recovery-target")
	for _, candidate := range allCorrupt {
		copied, copyErr := candidate.store.Copy(ctx, recovery.store, CopyRequest{SnapshotIDs: []string{candidate.snapshotID}})
		if copyErr == nil {
			t.Fatalf("Copy() accepted corrupt source %s: %#v", candidate.root, copied)
		}
		if len(copied.Mappings) != 0 {
			t.Fatalf("Copy() invented a mapping from corrupt source %s: %#v", candidate.root, copied)
		}
	}
}

func assertCorruptIntegrityError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Check() accepted physically corrupted pack data")
	}
	failure, ok := err.(*IntegrityError)
	if !ok || failure.IntegrityCode() != IntegrityCodeCorrupt || !failure.CorruptionVerified() || failure.IntegrityRetryable() {
		t.Fatalf("Check() corruption classification = %#v (%T)", err, err)
	}
}

func corruptRepositoryPack(t *testing.T, packPath string) {
	t.Helper()
	if err := os.Chmod(packPath, 0o600); err != nil {
		t.Fatalf("make pack writable for corruption fixture: %v", err)
	}
	pack, err := os.OpenFile(packPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open pack: %v", err)
	}
	info, err := pack.Stat()
	if err != nil {
		_ = pack.Close()
		t.Fatalf("stat pack: %v", err)
	}
	offset := info.Size() / 2
	byteValue := []byte{0}
	if _, err := pack.ReadAt(byteValue, offset); err != nil {
		_ = pack.Close()
		t.Fatalf("read pack byte: %v", err)
	}
	byteValue[0] ^= 0xff
	if _, err := pack.WriteAt(byteValue, offset); err != nil {
		_ = pack.Close()
		t.Fatalf("corrupt pack byte: %v", err)
	}
	if err := pack.Close(); err != nil {
		t.Fatalf("close pack: %v", err)
	}
}

func repositoryDataPack(t *testing.T, ctx context.Context, store *Store, repositoryPath string) string {
	t.Helper()
	repo, err := store.openRepository(ctx)
	if err != nil {
		t.Fatalf("open repository to select data pack: %v", err)
	}
	defer func() { _ = repo.Close() }()
	if err := repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory); err != nil {
		t.Fatalf("load repository index to select data pack: %v", err)
	}
	var packID restic.ID
	if err := repo.ListBlobs(ctx, func(blob restic.PackBlob) {
		if packID.IsNull() && blob.Handle().Type == restic.DataBlob {
			packID = blob.PackID()
		}
	}); err != nil {
		t.Fatalf("list repository blobs: %v", err)
	}
	if packID.IsNull() {
		t.Fatal("repository index contains no data pack")
	}
	id := packID.String()
	return filepath.Join(repositoryPath, "data", id[:2], id)
}

func findFilePayload(t *testing.T, root, expected string) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(contents) == expected {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk restore directory: %v", err)
	}
	return found
}

func findFileBytes(t *testing.T, root string, expected []byte) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(contents) == string(expected) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk restored bytes: %v", err)
	}
	return found
}

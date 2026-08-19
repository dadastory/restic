//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend/all"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

const (
	flashYunS3ARepository = "FLASHYUN_STORAGE_TEST_S3_A_REPOSITORY"
	flashYunS3BRepository = "FLASHYUN_STORAGE_TEST_S3_B_REPOSITORY"
	flashYunS3AccessKey   = "FLASHYUN_STORAGE_TEST_S3_ACCESS_KEY"
	flashYunS3SecretKey   = "FLASHYUN_STORAGE_TEST_S3_SECRET_KEY"
	flashYunRepoWorkers   = "FLASHYUN_STORAGE_TEST_MULTISTORE_REPOSITORIES"
)

type flashYunS3TestConfig struct {
	providerA string
	providerB string
	accessKey string
	secretKey string
	workers   int
}

func TestFlashYunS3DualWriteAndMigration(t *testing.T) {
	config := loadFlashYunS3TestConfig(t)
	t.Setenv("AWS_ACCESS_KEY_ID", config.accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.secretKey)
	repository.TestUseLowSecurityKDFParameters(t)
	restic.TestDisableCheckPolynomial(t)
	repository.TestSetLockTimeout(t, 0)

	base := t.TempDir()
	prefix := fmt.Sprintf("flashyun-%d", time.Now().UnixNano())
	migrationSource := flashYunS3Options(config, config.providerA+"/migration-source-"+prefix)
	migrationTarget := flashYunS3Options(config, config.providerB+"/migration-target-"+prefix)

	if err := flashYunInit(migrationSource, nil); err != nil {
		t.Fatalf("initialize migration source: %v", err)
	}
	migrationPayload := []byte("FlashYun migration payload: encrypted provider replication")
	migrationFile := filepath.Join(base, "migration", "payload.bin")
	if err := os.MkdirAll(filepath.Dir(migrationFile), 0o700); err != nil {
		t.Fatalf("create migration payload directory: %v", err)
	}
	if err := os.WriteFile(migrationFile, migrationPayload, 0o600); err != nil {
		t.Fatalf("write migration payload: %v", err)
	}
	if err := flashYunBackup(migrationSource, migrationFile); err != nil {
		t.Fatalf("backup migration source: %v", err)
	}
	if err := flashYunInit(migrationTarget, &migrationSource); err != nil {
		t.Fatalf("initialize migration target with source chunker parameters: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, config.workers+1)
	var operations sync.WaitGroup
	operations.Add(1)
	go func() {
		defer operations.Done()
		<-start
		if err := flashYunCopy(migrationSource, migrationTarget); err != nil {
			errs <- fmt.Errorf("migrate source from A to B: %w", err)
		}
	}()

	for worker := 0; worker < config.workers; worker++ {
		worker := worker
		payload := []byte(fmt.Sprintf("FlashYun dual-write payload %03d", worker))
		payloadFile := filepath.Join(base, "dual-write", fmt.Sprintf("repo-%03d", worker), "payload.bin")
		if err := os.MkdirAll(filepath.Dir(payloadFile), 0o700); err != nil {
			t.Fatalf("create payload directory for worker %d: %v", worker, err)
		}
		if err := os.WriteFile(payloadFile, payload, 0o600); err != nil {
			t.Fatalf("write payload for worker %d: %v", worker, err)
		}

		providerA := flashYunS3Options(config, config.providerA+fmt.Sprintf("/dual-a-%s-%03d", prefix, worker))
		providerB := flashYunS3Options(config, config.providerB+fmt.Sprintf("/dual-b-%s-%03d", prefix, worker))
		operations.Add(1)
		go func() {
			defer operations.Done()
			<-start
			if err := flashYunDualBackup(providerA, providerB, payloadFile); err != nil {
				errs <- fmt.Errorf("dual backup worker %d: %w", worker, err)
				return
			}
			if err := flashYunExpectSnapshots(providerA, 1); err != nil {
				errs <- fmt.Errorf("provider A worker %d: %w", worker, err)
			}
			if err := flashYunExpectSnapshots(providerB, 1); err != nil {
				errs <- fmt.Errorf("provider B worker %d: %w", worker, err)
			}
			if err := flashYunCheck(providerA); err != nil {
				errs <- fmt.Errorf("check provider A worker %d: %w", worker, err)
			}
			if err := flashYunCheck(providerB); err != nil {
				errs <- fmt.Errorf("check provider B worker %d: %w", worker, err)
			}
		}()
	}

	close(start)
	operations.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	if err := flashYunCheck(migrationTarget); err != nil {
		t.Fatalf("check migration target: %v", err)
	}
	if err := flashYunExpectSnapshots(migrationTarget, 1); err != nil {
		t.Fatalf("migration target snapshots: %v", err)
	}
	if err := flashYunCopy(migrationSource, migrationTarget); err != nil {
		t.Fatalf("repeat migration copy: %v", err)
	}
	if err := flashYunExpectSnapshots(migrationTarget, 1); err != nil {
		t.Fatalf("migration copy was not idempotent: %v", err)
	}

	restoreDir := filepath.Join(base, "restore")
	if err := flashYunRestore(migrationTarget, restoreDir); err != nil {
		t.Fatalf("restore migration target: %v", err)
	}
	if err := flashYunFindPayload(restoreDir, migrationPayload); err != nil {
		t.Fatalf("restored migration payload: %v", err)
	}
	if err := flashYunExpectSnapshots(migrationSource, 1); err != nil {
		t.Fatalf("migration source must be retained: %v", err)
	}
}

func TestFlashYunS3HistoricalSnapshotsAcrossRepositoryFormats(t *testing.T) {
	config := loadFlashYunS3TestConfig(t)
	t.Setenv("AWS_ACCESS_KEY_ID", config.accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.secretKey)
	repository.TestUseLowSecurityKDFParameters(t)
	restic.TestDisableCheckPolynomial(t)
	repository.TestSetLockTimeout(t, 0)

	versions := map[string][]byte{
		"v1": []byte("FlashYun historical version one"),
		"v2": []byte("FlashYun historical version two"),
		"v3": []byte("FlashYun historical version three"),
	}
	base := t.TempDir()
	prefix := fmt.Sprintf("flashyun-history-%d", time.Now().UnixNano())

	for _, repositoryVersion := range []string{"1", "2"} {
		repositoryVersion := repositoryVersion
		t.Run("repository-format-"+repositoryVersion, func(t *testing.T) {
			source := flashYunS3Options(config, config.providerA+"/history-source-"+repositoryVersion+"-"+prefix)
			target := flashYunS3Options(config, config.providerB+"/history-target-"+repositoryVersion+"-"+prefix)
			if err := flashYunInitWithVersion(source, nil, repositoryVersion); err != nil {
				t.Fatalf("initialize format %s source: %v", repositoryVersion, err)
			}

			path := filepath.Join(base, "history-"+repositoryVersion, "document.txt")
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatalf("create history directory: %v", err)
			}
			for _, version := range []string{"v1", "v2", "v3"} {
				if err := os.WriteFile(path, versions[version], 0o600); err != nil {
					t.Fatalf("write %s payload: %v", version, err)
				}
				if err := flashYunBackup(source, path); err != nil {
					t.Fatalf("backup %s: %v", version, err)
				}
			}

			if err := flashYunInitWithVersion(target, &source, repositoryVersion); err != nil {
				t.Fatalf("initialize format %s target: %v", repositoryVersion, err)
			}
			if err := flashYunCopy(source, target); err != nil {
				t.Fatalf("migrate format %s history: %v", repositoryVersion, err)
			}
			if err := flashYunCheck(target); err != nil {
				t.Fatalf("check format %s target: %v", repositoryVersion, err)
			}

			snapshotIDs, err := flashYunSnapshotIDs(target)
			if err != nil {
				t.Fatalf("list format %s snapshots: %v", repositoryVersion, err)
			}
			if len(snapshotIDs) != len(versions) {
				t.Fatalf("format %s target snapshot count = %d, want %d", repositoryVersion, len(snapshotIDs), len(versions))
			}

			remaining := make(map[string][]byte, len(versions))
			for name, payload := range versions {
				remaining[name] = payload
			}
			for _, snapshotID := range snapshotIDs {
				restoreDir := filepath.Join(base, "restore-"+repositoryVersion, snapshotID.String())
				if err := flashYunRestoreSnapshot(target, restoreDir, snapshotID.String()); err != nil {
					t.Fatalf("restore format %s snapshot %s: %v", repositoryVersion, snapshotID, err)
				}
				for name, payload := range remaining {
					if flashYunFindPayload(restoreDir, payload) == nil {
						delete(remaining, name)
						break
					}
				}
			}
			if len(remaining) != 0 {
				t.Fatalf("format %s did not restore all historical contents: %d missing", repositoryVersion, len(remaining))
			}
		})
	}
}

func loadFlashYunS3TestConfig(t *testing.T) flashYunS3TestConfig {
	t.Helper()
	config := flashYunS3TestConfig{
		providerA: os.Getenv(flashYunS3ARepository),
		providerB: os.Getenv(flashYunS3BRepository),
		accessKey: os.Getenv(flashYunS3AccessKey),
		secretKey: os.Getenv(flashYunS3SecretKey),
		workers:   24,
	}
	for _, name := range []string{flashYunS3ARepository, flashYunS3BRepository, flashYunS3AccessKey, flashYunS3SecretKey} {
		if os.Getenv(name) == "" {
			t.Skipf("%s is not configured; real multi-provider S3 verification was not requested", name)
		}
	}
	if value := os.Getenv(flashYunRepoWorkers); value != "" {
		workers, err := strconv.Atoi(value)
		if err != nil || workers < 2 || workers > 64 {
			t.Fatalf("%s must be an integer between 2 and 64, got %q", flashYunRepoWorkers, value)
		}
		config.workers = workers
	}
	return config
}

func flashYunS3Options(config flashYunS3TestConfig, repo string) global.Options {
	extended, err := options.Parse([]string{"s3.connections=16", "s3.bucket-lookup=path"})
	if err != nil {
		panic(err)
	}
	return global.Options{
		Repo:        repo,
		Password:    "flashyun-local-test-password",
		Quiet:       true,
		NoCache:     true,
		Compression: repository.CompressionFastest,
		Backends:    all.Backends(),
		Extended:    extended,
	}
}

func flashYunInit(destination global.Options, source *global.Options) error {
	return flashYunInitWithVersion(destination, source, "stable")
}

func flashYunInitWithVersion(destination global.Options, source *global.Options, repositoryVersion string) error {
	opts := InitOptions{RepositoryVersion: repositoryVersion}
	if source != nil {
		opts.CopyChunkerParameters = true
		opts.SecondaryRepoOptions = global.SecondaryRepoOptions{Repo: source.Repo, Password: source.Password}
	}
	return flashYunRun(destination, func(ctx context.Context, gopts global.Options) error {
		return runInit(ctx, opts, gopts, nil, gopts.Term)
	})
}

func flashYunDualBackup(providerA, providerB global.Options, target string) error {
	var operations sync.WaitGroup
	errs := make(chan error, 2)
	for _, provider := range []global.Options{providerA, providerB} {
		provider := provider
		operations.Add(1)
		go func() {
			defer operations.Done()
			if err := flashYunInit(provider, nil); err != nil {
				errs <- err
				return
			}
			errs <- flashYunBackup(provider, target)
		}()
	}
	operations.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func flashYunBackup(gopts global.Options, target string) error {
	return flashYunRun(gopts, func(ctx context.Context, gopts global.Options) error {
		opts := BackupOptions{GroupBy: data.SnapshotGroupByOptions{Host: true, Path: true}}
		return runBackup(ctx, opts, gopts, gopts.Term, []string{target})
	})
}

func flashYunCopy(source, destination global.Options) error {
	opts := CopyOptions{SecondaryRepoOptions: global.SecondaryRepoOptions{Repo: source.Repo, Password: source.Password}}
	return flashYunRun(destination, func(ctx context.Context, gopts global.Options) error {
		return runCopy(ctx, opts, gopts, nil, gopts.Term)
	})
}

func flashYunCheck(gopts global.Options) error {
	return flashYunRun(gopts, func(ctx context.Context, gopts global.Options) error {
		_, err := runCheck(ctx, CheckOptions{ReadData: true}, gopts, nil, gopts.Term)
		return err
	})
}

func flashYunExpectSnapshots(gopts global.Options, expected int) error {
	snapshotIDs, err := flashYunSnapshotIDs(gopts)
	if err != nil {
		return err
	}
	if len(snapshotIDs) != expected {
		return fmt.Errorf("snapshot count = %d, want %d", len(snapshotIDs), expected)
	}
	return nil
}

func flashYunSnapshotIDs(gopts global.Options) ([]restic.ID, error) {
	var snapshotIDs []restic.ID
	err := flashYunRun(gopts, func(ctx context.Context, gopts global.Options) error {
		ctx, repo, unlock, err := openWithReadLock(ctx, gopts, false, restic.NewNoopPrinter())
		if err != nil {
			return err
		}
		defer unlock()

		return repo.List(ctx, restic.SnapshotFile, func(id restic.ID, _ int64) error {
			snapshotIDs = append(snapshotIDs, id)
			return nil
		})
	})
	return snapshotIDs, err
}

func flashYunRestore(gopts global.Options, target string) error {
	return flashYunRestoreSnapshot(gopts, target, "latest")
}

func flashYunRestoreSnapshot(gopts global.Options, target string, snapshotID string) error {
	return flashYunRun(gopts, func(ctx context.Context, gopts global.Options) error {
		return runRestore(ctx, RestoreOptions{Target: target}, gopts, gopts.Term, []string{snapshotID})
	})
}

func flashYunRun(gopts global.Options, operation func(context.Context, global.Options) error) error {
	return withTermStatusRaw(os.Stdin, io.Discard, io.Discard, gopts, operation)
}

func flashYunFindPayload(root string, payload []byte) error {
	var found bool
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Equal(contents, payload) {
			found = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("payload not found below %s", root)
	}
	return nil
}

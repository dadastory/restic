package resticstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/restic/restic/internal/archiver"
	"github.com/restic/restic/internal/backend"
	backendcache "github.com/restic/restic/internal/backend/cache"
	"github.com/restic/restic/internal/backend/local"
	"github.com/restic/restic/internal/backend/s3"
	"github.com/restic/restic/internal/checker"
	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/dump"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/options"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
	"github.com/restic/restic/internal/restorer"
)

const (
	maxRepositoryKeys           = 20
	integrityLockRetryTimeout   = 2 * time.Minute
	MaxCopyVerifyBatchItems     = 16
	MaxReaderSnapshotBatchItems = 128
	MaxReaderSnapshotBatchBytes = int64(4 << 30)
	MaxRestoreBatchItems        = 128
	MaxRestoreBatchBytes        = int64(4 << 30)
)

var ErrRestoreBatchItemUnavailable = errors.New("restore batch item unavailable")

const (
	IntegrityCodeOpenUnavailable  = "repository_open_unavailable"
	IntegrityCodeLocked           = "repository_locked"
	IntegrityCodeTimeout          = "repository_check_timeout"
	IntegrityCodeCheckUnavailable = "repository_check_unavailable"
	IntegrityCodeCorrupt          = "repository_corrupt"
)

// IntegrityError is deliberately redacted and does not unwrap its backend
// cause. Callers receive only bounded classification metadata suitable for
// persistence, retry policy and corruption repair decisions.
type IntegrityError struct {
	code               string
	retryable          bool
	corruptionVerified bool
}

func (failure *IntegrityError) Error() string {
	return "restic repository integrity check failed: " + failure.code
}
func (failure *IntegrityError) IntegrityCode() string    { return failure.code }
func (failure *IntegrityError) IntegrityRetryable() bool { return failure.retryable }
func (failure *IntegrityError) CorruptionVerified() bool { return failure.corruptionVerified }

func newIntegrityError(code string, retryable, corruptionVerified bool) error {
	return &IntegrityError{code: code, retryable: retryable, corruptionVerified: corruptionVerified}
}

// Restic calibrates repository KDF parameters through package state during
// initialization. Serialize only that initialization section while preserving
// cancellation for callers waiting to initialize distinct providers.
var repositoryInitializationPermit = func() chan struct{} {
	permit := make(chan struct{}, 1)
	permit <- struct{}{}
	return permit
}()

// Store is a public Restic repository facade. It owns no global process state:
// several Store instances with distinct S3 credentials can run concurrently.
type Store struct {
	config                     Config
	observeCopySession         func(string)
	observeReaderBatchSession  func(string)
	observeRestoreBatchSession func(string)
}

// SnapshotRequest identifies a server-owned staged source directory.
type SnapshotRequest struct {
	SourcePath       string
	Hostname         string
	Tags             []string
	ReadConcurrency  uint
	WriteConcurrency uint
	SnapshotTime     time.Time
}

// ReaderSnapshotRequest describes one immutable, reopenable provider object.
// Open must return a fresh reader at offset zero and must not expose Provider
// credentials or a node-local complete-file path.
type ReaderSnapshotRequest struct {
	Name             string
	Size             int64
	ExpectedChecksum string
	IdempotencyKey   string
	Open             func(context.Context) (io.ReadCloser, error)
	Hostname         string
	Tags             []string
	ReadConcurrency  uint
	WriteConcurrency uint
	SnapshotTime     time.Time
	// ReconcileExisting is set only when the caller has durable evidence that
	// an earlier external effect may have committed without recording its
	// result. New effects keep this false so ordinary writes never enumerate
	// unrelated snapshot history.
	ReconcileExisting bool
}

// ReaderSnapshotBatchRequest groups independent one-file snapshots that share
// one physical repository/uploader session. Every item still receives its own
// snapshot identity and operation tag.
type ReaderSnapshotBatchRequest struct {
	Items []ReaderSnapshotRequest
}

// ReaderSnapshotItemResult keeps source failures attributable without making
// another successful file share its publication outcome.
type ReaderSnapshotItemResult struct {
	Snapshot SnapshotResult
	Err      error
}

type ReaderSnapshotBatchResult struct {
	Items []ReaderSnapshotItemResult
}

// SnapshotResult contains server-generated immutable metadata for a snapshot.
type SnapshotResult struct {
	ID             string
	ProcessedBytes uint64
	DataAddedBytes uint64
	StartedAt      time.Time
	CompletedAt    time.Time
	Checksum       string
}

// VerifySnapshotPayloadRequest identifies one exact, server-recorded snapshot
// and its immutable payload evidence. SnapshotID must be a full Restic ID; tag
// or latest-style selection is deliberately unsupported.
type VerifySnapshotPayloadRequest struct {
	SnapshotID       string
	ExpectedSize     int64
	ExpectedChecksum string
}

// PayloadVerification is the evidence measured while Restic decrypts and
// authenticates the exact payload stream.
type PayloadVerification struct {
	Size     int64
	Checksum string
}

// CopyVerifyItem identifies one exact source snapshot and the immutable
// payload evidence that must be proven on the destination repository.
type CopyVerifyItem struct {
	SnapshotID       string
	ExpectedSize     int64
	ExpectedChecksum string
}

// CopyVerifyBatchRequest is deliberately bounded. CreateOnly applies to the
// whole session so uncertain reconciliation is never mixed with a proven-new
// external effect.
type CopyVerifyBatchRequest struct {
	Items            []CopyVerifyItem
	CreateOnly       bool
	WriteConcurrency uint
}

// CopyVerifyItemResult preserves a created mapping even when its exact target
// payload proof fails. Callers can durably record the external effect before
// rejecting publication and recording integrity evidence.
type CopyVerifyItemResult struct {
	Mapping      SnapshotMapping
	Verification PayloadVerification
	Err          error
}

type CopyVerifyBatchResult struct {
	Items []CopyVerifyItemResult
}

// SnapshotReader archives a single provider-staged object without creating a
// second complete local work file. Restic reads from a one-file virtual FS;
// size and optional SHA-256 expectations are checked while bytes stream.
func (s *Store) SnapshotReader(ctx context.Context, request ReaderSnapshotRequest) (SnapshotResult, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotResult{}, err
	}
	operationTag, err := readerOperationTag(request.IdempotencyKey)
	if err != nil || validateReaderSnapshotRequest(request) != nil {
		return SnapshotResult{}, fmt.Errorf("invalid reader snapshot request")
	}
	if request.ReconcileExisting {
		existing, found, findErr := s.findSnapshotByTag(ctx, operationTag)
		if findErr != nil {
			return SnapshotResult{}, findErr
		}
		if found {
			checksum, validateErr := measureReaderSource(ctx, request)
			if validateErr != nil {
				return SnapshotResult{}, s.operationError("validate existing reader snapshot", validateErr)
			}
			existing.Checksum = checksum
			return existing, nil
		}
	}
	reader, err := request.Open(ctx)
	if err != nil {
		return SnapshotResult{}, s.operationError("open snapshot reader", err)
	}
	measured := newMeasuredReader(ctx, reader, request.Size, request.ExpectedChecksum)
	filesystem, err := fs.NewReader(request.Name, measured, fs.ReaderOptions{Mode: 0o600, ModTime: snapshotTime(request.SnapshotTime), Size: request.Size, AllowEmptyFile: true})
	if err != nil {
		_ = measured.Close()
		return SnapshotResult{}, fmt.Errorf("create snapshot reader filesystem: %w", err)
	}
	tags := append([]string(nil), request.Tags...)
	tags = append(tags, operationTag)
	result, err := s.snapshotFilesystem(ctx, filesystem, []string{"/" + request.Name}, SnapshotRequest{Hostname: request.Hostname, Tags: tags, ReadConcurrency: request.ReadConcurrency, WriteConcurrency: request.WriteConcurrency, SnapshotTime: request.SnapshotTime})
	if err != nil {
		return SnapshotResult{}, err
	}
	if err := measured.Validate(); err != nil {
		return SnapshotResult{}, s.operationError("validate snapshot reader", err)
	}
	result.Checksum = measured.Checksum()
	return result, nil
}

// SnapshotReadersBatch creates independent one-file snapshots while sharing a
// single repository open, index load, append lock and blob uploader flush.
func (s *Store) SnapshotReadersBatch(ctx context.Context, request ReaderSnapshotBatchRequest) (ReaderSnapshotBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return ReaderSnapshotBatchResult{}, err
	}
	if len(request.Items) == 0 || len(request.Items) > MaxReaderSnapshotBatchItems {
		return ReaderSnapshotBatchResult{}, fmt.Errorf("invalid reader snapshot batch request")
	}
	var totalBytes int64
	operationTags := make([]string, len(request.Items))
	seen := make(map[string]struct{}, len(request.Items))
	writeConcurrency := request.Items[0].WriteConcurrency
	for index, item := range request.Items {
		if validateReaderSnapshotRequest(item) != nil || item.WriteConcurrency != writeConcurrency || item.Size > MaxReaderSnapshotBatchBytes-totalBytes {
			return ReaderSnapshotBatchResult{}, fmt.Errorf("invalid reader snapshot batch request")
		}
		tag, err := readerOperationTag(item.IdempotencyKey)
		if err != nil {
			return ReaderSnapshotBatchResult{}, fmt.Errorf("invalid reader snapshot batch request")
		}
		if _, duplicate := seen[tag]; duplicate {
			return ReaderSnapshotBatchResult{}, fmt.Errorf("invalid reader snapshot batch request")
		}
		seen[tag] = struct{}{}
		operationTags[index] = tag
		totalBytes += item.Size
	}

	results := ReaderSnapshotBatchResult{Items: make([]ReaderSnapshotItemResult, len(request.Items))}
	repo, err := s.openRepositoryWithConnections(ctx, writeConcurrency)
	if err != nil {
		return results, err
	}
	defer func() { _ = repo.Close() }()
	s.observeReaderBatch("repository_opened")
	if err := repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory); err != nil {
		return results, s.operationError("load repository index", err)
	}
	s.observeReaderBatch("index_loaded")
	unlock, lockedCtx, err := repository.LockRepo(ctx, repo, false, 0, func(string) {}, func(string, ...interface{}) {})
	if err != nil {
		return results, s.operationError("acquire repository append lock", err)
	}
	defer unlock()
	s.observeReaderBatch("repository_locked")

	resolved := make([]bool, len(request.Items))
	if hasReconcileReaderItem(request.Items) {
		existing, findErr := findSnapshotsByOperationTags(lockedCtx, repo, seen)
		if findErr != nil {
			return results, s.operationError("find reader snapshots", findErr)
		}
		for index, item := range request.Items {
			if !item.ReconcileExisting {
				continue
			}
			found, ok := existing[operationTags[index]]
			if !ok {
				continue
			}
			checksum, measureErr := measureReaderSource(lockedCtx, item)
			if measureErr != nil {
				results.Items[index].Err = s.operationError("validate existing reader snapshot", measureErr)
				resolved[index] = true
				continue
			}
			found.Checksum = checksum
			results.Items[index].Snapshot = found
			resolved[index] = true
		}
	}

	type pendingSnapshot struct {
		index    int
		snapshot *data.Snapshot
		summary  *archiver.Summary
		checksum string
	}
	pending := make([]pendingSnapshot, 0, len(request.Items))
	err = repo.WithBlobUploader(lockedCtx, func(uploadCtx context.Context, uploader restic.BlobSaverWithAsync) error {
		s.observeReaderBatch("uploader_started")
		for index, item := range request.Items {
			if resolved[index] {
				continue
			}
			if err := uploadCtx.Err(); err != nil {
				results.Items[index].Err = err
				continue
			}
			reader, openErr := item.Open(uploadCtx)
			if openErr != nil {
				results.Items[index].Err = s.operationError("open snapshot reader", openErr)
				continue
			}
			measured := newMeasuredReader(uploadCtx, reader, item.Size, item.ExpectedChecksum)
			filesystem, fsErr := fs.NewReader(item.Name, measured, fs.ReaderOptions{Mode: 0o600, ModTime: snapshotTime(item.SnapshotTime), Size: item.Size, AllowEmptyFile: true})
			if fsErr != nil {
				_ = measured.Close()
				results.Items[index].Err = fmt.Errorf("create snapshot reader filesystem: %w", fsErr)
				continue
			}
			tags := append([]string(nil), item.Tags...)
			tags = append(tags, operationTags[index])
			now := snapshotTime(item.SnapshotTime)
			archive := archiver.New(repo, filesystem, archiver.Options{ReadConcurrency: item.ReadConcurrency})
			archive.Error = func(_ string, err error) error { return err }
			snapshot, summary, snapshotErr := archive.SnapshotWithUploader(uploadCtx, []string{"/" + item.Name}, archiver.SnapshotOptions{Tags: data.TagList(tags), Hostname: item.Hostname, BackupStart: now, Time: now, ProgramVersion: "flashyun-resticstore"}, uploader)
			if snapshotErr == nil {
				snapshotErr = measured.Validate()
			}
			if snapshotErr != nil {
				results.Items[index].Err = s.operationError("create reader snapshot tree", snapshotErr)
				continue
			}
			pending = append(pending, pendingSnapshot{index: index, snapshot: snapshot, summary: summary, checksum: measured.Checksum()})
		}
		return nil
	})
	if err != nil {
		return results, s.operationError("flush reader snapshot batch", err)
	}
	s.observeReaderBatch("uploader_flushed")
	for _, item := range pending {
		id, saveErr := data.SaveSnapshot(lockedCtx, repo, item.snapshot)
		if saveErr != nil {
			results.Items[item.index].Err = s.operationError("save reader snapshot", saveErr)
			continue
		}
		results.Items[item.index].Snapshot = SnapshotResult{ID: id.String(), ProcessedBytes: item.summary.ProcessedBytes, DataAddedBytes: item.summary.DataSizeInRepo + item.summary.TreeSizeInRepo, StartedAt: item.summary.BackupStart.UTC(), CompletedAt: item.summary.BackupEnd.UTC(), Checksum: item.checksum}
	}
	s.observeReaderBatch("snapshots_saved")
	return results, nil
}

func validateReaderSnapshotRequest(request ReaderSnapshotRequest) error {
	if request.Name != "payload" || request.Size < 0 || request.Open == nil || strings.TrimSpace(request.Hostname) == "" || !validExpectedChecksum(request.ExpectedChecksum) || request.ReadConcurrency > 64 || request.WriteConcurrency > 64 {
		return fmt.Errorf("invalid reader snapshot request")
	}
	return nil
}

func hasReconcileReaderItem(items []ReaderSnapshotRequest) bool {
	for _, item := range items {
		if item.ReconcileExisting {
			return true
		}
	}
	return false
}

func findSnapshotsByOperationTags(ctx context.Context, repo *repository.Repository, tags map[string]struct{}) (map[string]SnapshotResult, error) {
	results := make(map[string]SnapshotResult, len(tags))
	err := data.ForAllSnapshots(ctx, repo, repo, nil, func(id restic.ID, snapshot *data.Snapshot, loadErr error) error {
		if loadErr != nil {
			return loadErr
		}
		for _, tag := range snapshot.Tags {
			if _, wanted := tags[tag]; !wanted {
				continue
			}
			if _, duplicate := results[tag]; duplicate {
				return fmt.Errorf("multiple snapshots share one operation key")
			}
			result := SnapshotResult{ID: id.String()}
			if snapshot.Summary != nil {
				result.ProcessedBytes = snapshot.Summary.TotalBytesProcessed
				result.DataAddedBytes = snapshot.Summary.DataAdded
				result.StartedAt = snapshot.Summary.BackupStart.UTC()
				result.CompletedAt = snapshot.Summary.BackupEnd.UTC()
			}
			results[tag] = result
		}
		return nil
	})
	return results, err
}

func (s *Store) observeReaderBatch(stage string) {
	if s != nil && s.observeReaderBatchSession != nil {
		s.observeReaderBatchSession(stage)
	}
}

func readerOperationTag(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) < 8 || len(value) > 256 {
		return "", fmt.Errorf("invalid reader operation key")
	}
	digest := sha256.Sum256([]byte(value))
	return "flashyun-operation:" + hex.EncodeToString(digest[:]), nil
}

func measureReaderSource(ctx context.Context, request ReaderSnapshotRequest) (string, error) {
	reader, err := request.Open(ctx)
	if err != nil {
		return "", err
	}
	measured := newMeasuredReader(ctx, reader, request.Size, request.ExpectedChecksum)
	_, copyErr := io.Copy(io.Discard, measured)
	closeErr := measured.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if err := measured.Validate(); err != nil {
		return "", err
	}
	return measured.Checksum(), nil
}

func (s *Store) findSnapshotByTag(ctx context.Context, tag string) (SnapshotResult, bool, error) {
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		return SnapshotResult{}, false, err
	}
	defer func() {
		unlock()
		_ = repo.Close()
	}()
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return SnapshotResult{}, false, s.operationError("load repository index", err)
	}
	var result SnapshotResult
	found := false
	err = data.ForAllSnapshots(lockedCtx, repo, repo, nil, func(id restic.ID, snapshot *data.Snapshot, loadErr error) error {
		if loadErr != nil {
			return loadErr
		}
		if !snapshot.HasTags([]string{tag}) {
			return nil
		}
		if found {
			return fmt.Errorf("multiple snapshots share one operation key")
		}
		found = true
		result.ID = id.String()
		if snapshot.Summary != nil {
			result.ProcessedBytes = snapshot.Summary.TotalBytesProcessed
			result.DataAddedBytes = snapshot.Summary.DataAdded
			result.StartedAt = snapshot.Summary.BackupStart.UTC()
			result.CompletedAt = snapshot.Summary.BackupEnd.UTC()
		}
		return nil
	})
	if err != nil {
		return SnapshotResult{}, false, s.operationError("find reader snapshot", err)
	}
	return result, found, nil
}

func (s *Store) snapshotFilesystem(ctx context.Context, filesystem fs.FS, targets []string, request SnapshotRequest) (SnapshotResult, error) {
	if request.ReadConcurrency > 64 || request.WriteConcurrency > 64 {
		return SnapshotResult{}, fmt.Errorf("snapshot concurrency exceeds typed limit")
	}
	repo, err := s.openRepositoryWithConnections(ctx, request.WriteConcurrency)
	if err != nil {
		return SnapshotResult{}, err
	}
	defer func() { _ = repo.Close() }()
	if err := repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory); err != nil {
		return SnapshotResult{}, s.operationError("load repository index", err)
	}
	unlock, lockedCtx, err := repository.LockRepo(ctx, repo, false, 0, func(string) {}, func(string, ...interface{}) {})
	if err != nil {
		return SnapshotResult{}, s.operationError("acquire repository append lock", err)
	}
	defer unlock()
	now := snapshotTime(request.SnapshotTime)
	archive := archiver.New(repo, filesystem, archiver.Options{ReadConcurrency: request.ReadConcurrency})
	archive.Error = func(_ string, err error) error { return err }
	snapshot, id, summary, err := archive.Snapshot(lockedCtx, targets, archiver.SnapshotOptions{Tags: data.TagList(request.Tags), Hostname: request.Hostname, BackupStart: now, Time: now, ProgramVersion: "flashyun-resticstore"})
	if err != nil {
		return SnapshotResult{}, s.operationError("create snapshot", err)
	}
	if snapshot == nil || id.IsNull() || summary == nil {
		return SnapshotResult{}, fmt.Errorf("create snapshot returned incomplete metadata")
	}
	return SnapshotResult{ID: id.String(), ProcessedBytes: summary.ProcessedBytes, DataAddedBytes: summary.DataSizeInRepo + summary.TreeSizeInRepo, StartedAt: summary.BackupStart.UTC(), CompletedAt: summary.BackupEnd.UTC()}, nil
}

func snapshotTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

func validExpectedChecksum(value string) bool {
	if value == "" {
		return true
	}
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

type measuredReader struct {
	ctx      context.Context
	reader   io.ReadCloser
	expected int64
	wantHash string
	hash     hash.Hash
	read     int64
	checked  bool
	checkErr error
}

func newMeasuredReader(ctx context.Context, reader io.ReadCloser, expected int64, wantHash string) *measuredReader {
	return &measuredReader{ctx: ctx, reader: reader, expected: expected, wantHash: wantHash, hash: sha256.New()}
}

func (reader *measuredReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	count, err := reader.reader.Read(buffer)
	if count > 0 {
		reader.read += int64(count)
		_, _ = reader.hash.Write(buffer[:count])
	}
	if errors.Is(err, io.EOF) {
		if validationErr := reader.Validate(); validationErr != nil {
			return count, validationErr
		}
	}
	return count, err
}

func (reader *measuredReader) Close() error {
	return reader.reader.Close()
}

func (reader *measuredReader) Validate() error {
	if reader.checked {
		return reader.checkErr
	}
	reader.checked = true
	checksum := reader.Checksum()
	if reader.read != reader.expected {
		reader.checkErr = fmt.Errorf("snapshot reader size mismatch")
	} else if reader.wantHash != "" && reader.wantHash != checksum {
		reader.checkErr = fmt.Errorf("snapshot reader checksum mismatch")
	}
	return reader.checkErr
}

func (reader *measuredReader) Checksum() string {
	return "sha256:" + hex.EncodeToString(reader.hash.Sum(nil))
}

// StatsResult is the measured physical repository footprint for one provider.
// It intentionally reports no provider credentials or storage location.
type StatsResult struct {
	SnapshotCount   uint64
	RepositoryBytes uint64
}

// RestoreRequest identifies one immutable snapshot and a server-controlled
// destination directory.
type RestoreRequest struct {
	SnapshotID      string
	DestinationPath string
}

// RestoreBatchItem identifies one immutable snapshot and an isolated,
// server-controlled destination. ExpectedSize is used only for admission; the
// caller remains responsible for business checksum and exact-size proof.
type RestoreBatchItem struct {
	SnapshotID      string
	DestinationPath string
	ExpectedSize    int64
}

type RestoreBatchRequest struct {
	Items []RestoreBatchItem
}

type RestoreBatchItemResult struct {
	Err error
}

type RestoreBatchResult struct {
	Items []RestoreBatchItemResult
}

// CopyRequest selects source snapshots to copy. An empty SnapshotIDs list
// copies every source snapshot. Snapshot data is decrypted and re-encrypted;
// callers must supply distinct source and destination stores.
type CopyRequest struct {
	SnapshotIDs      []string
	WriteConcurrency uint
	// CreateOnly is safe only when the caller has durably proved this external
	// effect has never run. The default preserves resumable Original mapping
	// reconciliation for uncertain and legacy callers.
	CreateOnly bool
}

// CopyResult reports destination snapshot IDs created by Copy. Mappings always
// include every selected source snapshot, including previously copied snapshots
// that were skipped during a resumable retry.
type CopyResult struct {
	CopiedSnapshots  uint64
	SkippedSnapshots uint64
	SnapshotIDs      []string
	Mappings         []SnapshotMapping
}

// SnapshotMapping records the immutable source snapshot and its corresponding
// snapshot in the destination repository. It is safe for callers to persist
// this value as an application-level recovery fallback.
type SnapshotMapping struct {
	SourceSnapshotID      string
	ReplacementSnapshotID string
}

// RetentionRequest describes one server-planned retention operation. Legacy
// callers may retain KeepLast repository snapshots; application retention must
// supply ForgetSnapshotIDs so it can delete only version snapshots whose
// metadata lifecycle has already reached the purge stage.
type RetentionRequest struct {
	KeepLast             uint64
	ForgetSnapshotIDs    []string
	ProtectedSnapshotIDs []string
}

// RetentionResult reports the outcome of one successful forget-and-prune pass.
type RetentionResult struct {
	KeptSnapshots      uint64
	ForgottenSnapshots uint64
}

// New validates and copies one provider configuration.
func New(config Config) (*Store, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &Store{config: cloneConfig(config)}, nil
}

// Initialize creates a new Restic repository on the configured provider.
func (s *Store) Initialize(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := acquireInitializationPermit(ctx, repositoryInitializationPermit); err != nil {
		return err
	}
	defer func() { repositoryInitializationPermit <- struct{}{} }()
	be, err := s.backend(ctx, true)
	if err != nil {
		return s.operationError("initialize provider", err)
	}
	repo, err := repository.New(be, repository.Options{Compression: repository.CompressionFastest})
	if err != nil {
		return s.operationError("create repository", err)
	}
	defer func() { _ = repo.Close() }()
	if err := repo.Init(ctx, restic.StableRepoVersion, s.config.RepositoryPassword, nil); err != nil {
		return s.operationError("initialize repository", err)
	}
	return nil
}

// Snapshot archives a server-owned staged directory under an append repository
// lock and returns only generated snapshot metadata.
func (s *Store) Snapshot(ctx context.Context, request SnapshotRequest) (SnapshotResult, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotResult{}, err
	}
	if request.SourcePath == "" {
		return SnapshotResult{}, fmt.Errorf("snapshot source path is required")
	}
	info, err := os.Stat(request.SourcePath)
	if err != nil {
		return SnapshotResult{}, s.operationError("stat snapshot source", err)
	}
	if !info.IsDir() {
		return SnapshotResult{}, fmt.Errorf("snapshot source path must be a directory")
	}

	repo, err := s.openRepository(ctx)
	if err != nil {
		return SnapshotResult{}, err
	}
	defer func() { _ = repo.Close() }()
	if err := repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory); err != nil {
		return SnapshotResult{}, s.operationError("load repository index", err)
	}

	unlock, lockedCtx, err := repository.LockRepo(ctx, repo, false, 0, func(string) {}, func(string, ...interface{}) {})
	if err != nil {
		return SnapshotResult{}, s.operationError("acquire repository append lock", err)
	}
	defer unlock()

	now := request.SnapshotTime
	if now.IsZero() {
		now = time.Now().UTC()
	}
	archive := newSnapshotArchiver(repo, request)
	snapshot, id, summary, err := archive.Snapshot(lockedCtx, []string{request.SourcePath}, archiver.SnapshotOptions{
		Tags:           data.TagList(request.Tags),
		Hostname:       request.Hostname,
		BackupStart:    now,
		Time:           now,
		ProgramVersion: "flashyun-resticstore",
	})
	if err != nil {
		return SnapshotResult{}, s.operationError("create snapshot", err)
	}
	if snapshot == nil || id.IsNull() || summary == nil {
		return SnapshotResult{}, fmt.Errorf("create snapshot returned incomplete metadata")
	}
	return SnapshotResult{
		ID:             id.String(),
		ProcessedBytes: summary.ProcessedBytes,
		DataAddedBytes: summary.DataSizeInRepo + summary.TreeSizeInRepo,
		StartedAt:      summary.BackupStart.UTC(),
		CompletedAt:    summary.BackupEnd.UTC(),
	}, nil
}

func newSnapshotArchiver(repo *repository.Repository, request SnapshotRequest) *archiver.Archiver {
	archive := archiver.New(repo, fs.NewLocal(), archiver.Options{ReadConcurrency: request.ReadConcurrency})
	// The Restic CLI always installs this callback. Embedded callers must do the
	// same because tree-saver workers invoke it directly when a child read or
	// context operation fails.
	archive.Error = func(_ string, err error) error { return err }
	return archive
}

// Restore restores exactly one snapshot to a server-controlled destination.
func (s *Store) Restore(ctx context.Context, request RestoreRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.SnapshotID == "" {
		return fmt.Errorf("restore snapshot ID is required")
	}
	if request.DestinationPath == "" || !filepath.IsAbs(request.DestinationPath) {
		return fmt.Errorf("restore destination path must be absolute")
	}
	snapshotID, err := restic.ParseID(request.SnapshotID)
	if err != nil {
		return fmt.Errorf("restore snapshot ID is invalid")
	}

	repo, err := s.openRepository(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = repo.Close() }()
	unlock, lockedCtx, err := repository.LockRepo(ctx, repo, false, 0, func(string) {}, func(string, ...interface{}) {})
	if err != nil {
		return s.operationError("acquire repository read lock", err)
	}
	defer unlock()
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return s.operationError("load repository index", err)
	}
	snapshot, err := data.LoadSnapshot(ctx, repo, snapshotID)
	if err != nil {
		return s.operationError("load snapshot", err)
	}
	if snapshot.Tree == nil {
		return fmt.Errorf("restore snapshot has no tree")
	}
	// A drive restore must not require privileged ownership changes in a
	// rootless API container; the destination is owned by the API service.
	restorer := restorer.NewRestorer(repo, snapshot, restorer.Options{
		Overwrite:     restorer.OverwriteAlways,
		SkipOwnership: true,
	})
	if _, err := restorer.RestoreTo(lockedCtx, request.DestinationPath); err != nil {
		return s.operationError("restore snapshot", err)
	}
	return nil
}

// RestoreSnapshotsBatch restores independent snapshots while sharing one
// repository open, read lock and loaded index. Item destinations are isolated
// so one failed restore can be removed without invalidating completed items.
func (s *Store) RestoreSnapshotsBatch(ctx context.Context, request RestoreBatchRequest) (RestoreBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return RestoreBatchResult{}, err
	}
	if len(request.Items) == 0 || len(request.Items) > MaxRestoreBatchItems {
		return RestoreBatchResult{}, fmt.Errorf("invalid restore batch request")
	}
	ids := make([]restic.ID, len(request.Items))
	seenDestinations := make(map[string]struct{}, len(request.Items))
	var totalBytes int64
	for index, item := range request.Items {
		destination := filepath.Clean(item.DestinationPath)
		id, err := restic.ParseID(item.SnapshotID)
		if err != nil || !filepath.IsAbs(destination) || destination == string(filepath.Separator) || item.ExpectedSize < 0 || item.ExpectedSize > MaxRestoreBatchBytes-totalBytes {
			return RestoreBatchResult{}, fmt.Errorf("invalid restore batch request")
		}
		if _, duplicate := seenDestinations[destination]; duplicate {
			return RestoreBatchResult{}, fmt.Errorf("invalid restore batch request")
		}
		seenDestinations[destination] = struct{}{}
		ids[index] = id
		totalBytes += item.ExpectedSize
	}

	result := RestoreBatchResult{Items: make([]RestoreBatchItemResult, len(request.Items))}
	repo, err := s.openRepository(ctx)
	if err != nil {
		return result, err
	}
	defer func() { _ = repo.Close() }()
	s.observeRestoreBatch("repository_opened")
	unlock, lockedCtx, err := repository.LockRepo(ctx, repo, false, 0, func(string) {}, func(string, ...interface{}) {})
	if err != nil {
		return result, s.operationError("acquire repository read lock", err)
	}
	defer unlock()
	s.observeRestoreBatch("repository_locked")
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return result, s.operationError("load repository index", err)
	}
	s.observeRestoreBatch("index_loaded")

	for index, item := range request.Items {
		if err := lockedCtx.Err(); err != nil {
			for remaining := index; remaining < len(result.Items); remaining++ {
				result.Items[remaining].Err = err
				_ = os.RemoveAll(request.Items[remaining].DestinationPath)
			}
			break
		}
		if err := os.MkdirAll(item.DestinationPath, 0o700); err != nil {
			result.Items[index].Err = ErrRestoreBatchItemUnavailable
			_ = os.RemoveAll(item.DestinationPath)
			continue
		}
		snapshot, err := data.LoadSnapshot(lockedCtx, repo, ids[index])
		if err == nil && snapshot.Tree == nil {
			err = fmt.Errorf("restore snapshot has no tree")
		}
		if err == nil {
			restore := restorer.NewRestorer(repo, snapshot, restorer.Options{Overwrite: restorer.OverwriteAlways, SkipOwnership: true})
			_, err = restore.RestoreTo(lockedCtx, item.DestinationPath)
		}
		if err != nil {
			result.Items[index].Err = ErrRestoreBatchItemUnavailable
			_ = os.RemoveAll(item.DestinationPath)
		}
	}
	s.observeRestoreBatch("snapshots_restored")
	return result, nil
}

func (s *Store) observeRestoreBatch(stage string) {
	if s != nil && s.observeRestoreBatchSession != nil {
		s.observeRestoreBatchSession(stage)
	}
}

// VerifySnapshotPayload streams the unique root-level payload file from one
// exact snapshot. It does not materialize the file and does not scan unrelated
// repository packs or snapshots.
func (s *Store) VerifySnapshotPayload(ctx context.Context, request VerifySnapshotPayloadRequest) (PayloadVerification, error) {
	if err := ctx.Err(); err != nil {
		return PayloadVerification{}, err
	}
	if err := validatePayloadVerificationRequest(request); err != nil {
		return PayloadVerification{}, fmt.Errorf("verify payload snapshot ID is invalid")
	}
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		return PayloadVerification{}, err
	}
	defer unlock()
	defer func() { _ = repo.Close() }()
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return PayloadVerification{}, s.operationError("load repository index", err)
	}
	return s.verifySnapshotPayloadInRepository(lockedCtx, repo, request)
}

func validatePayloadVerificationRequest(request VerifySnapshotPayloadRequest) error {
	if request.ExpectedSize < 0 || !validExpectedChecksum(request.ExpectedChecksum) {
		return fmt.Errorf("invalid payload verification request")
	}
	if _, err := restic.ParseID(strings.TrimSpace(request.SnapshotID)); err != nil {
		return err
	}
	return nil
}

func (s *Store) verifySnapshotPayloadInRepository(ctx context.Context, repo *repository.Repository, request VerifySnapshotPayloadRequest) (PayloadVerification, error) {
	snapshotID, err := restic.ParseID(strings.TrimSpace(request.SnapshotID))
	if err != nil {
		return PayloadVerification{}, fmt.Errorf("verify payload snapshot ID is invalid")
	}
	snapshot, err := data.LoadSnapshot(ctx, repo, snapshotID)
	if err != nil {
		return PayloadVerification{}, s.operationError("load snapshot payload", err)
	}
	if snapshot.Tree == nil {
		return PayloadVerification{}, fmt.Errorf("verify snapshot payload has no tree")
	}
	tree, err := data.LoadTree(ctx, repo, *snapshot.Tree)
	if err != nil {
		return PayloadVerification{}, s.operationError("load snapshot payload tree", err)
	}
	finder := data.NewTreeFinder(tree)
	defer finder.Close()
	node, err := finder.Find("payload")
	if err != nil {
		return PayloadVerification{}, s.operationError("find snapshot payload", err)
	}
	if node == nil || node.Type != data.NodeTypeFile {
		return PayloadVerification{}, fmt.Errorf("verify snapshot payload is missing")
	}
	measured := &payloadVerificationWriter{hash: sha256.New()}
	if err := dump.New("", repo, measured).WriteNode(ctx, node); err != nil {
		return PayloadVerification{}, s.operationError("stream snapshot payload", err)
	}
	checksum := "sha256:" + hex.EncodeToString(measured.hash.Sum(nil))
	if measured.size != request.ExpectedSize {
		return PayloadVerification{}, fmt.Errorf("verify snapshot payload size mismatch")
	}
	if request.ExpectedChecksum != "" && !strings.EqualFold(checksum, request.ExpectedChecksum) {
		return PayloadVerification{}, fmt.Errorf("verify snapshot payload checksum mismatch")
	}
	return PayloadVerification{Size: measured.size, Checksum: checksum}, ctx.Err()
}

type payloadVerificationWriter struct {
	hash hash.Hash
	size int64
}

func (writer *payloadVerificationWriter) Write(payload []byte) (int, error) {
	written, err := writer.hash.Write(payload)
	writer.size += int64(written)
	return written, err
}

// VerifySnapshots confirms that each full, server-recorded snapshot ID is
// present and readable. It deliberately does not restore file data; callers
// use it as the non-destructive prerequisite for retention maintenance.
func (s *Store) VerifySnapshots(ctx context.Context, snapshotIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(snapshotIDs) == 0 {
		return nil
	}
	ids := restic.NewIDSet()
	for _, rawID := range snapshotIDs {
		id, err := restic.ParseID(strings.TrimSpace(rawID))
		if err != nil {
			return fmt.Errorf("verify snapshot ID is invalid")
		}
		ids.Insert(id)
	}
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		return err
	}
	defer unlock()
	defer func() { _ = repo.Close() }()
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return s.operationError("load repository index", err)
	}
	for id := range ids {
		if _, err := data.LoadSnapshot(lockedCtx, repo, id); err != nil {
			return s.operationError("verify snapshot", err)
		}
	}
	return lockedCtx.Err()
}

// Copy migrates selected immutable snapshots to another initialized Store.
// The destination receives new snapshot IDs with the source snapshot recorded
// as Original, allowing a retry to safely skip already migrated content.
type copySession struct {
	source, destination         *Store
	sourceRepo, destinationRepo *repository.Repository
	ctx                         context.Context
	cleanup                     func()
	destinationMappings         map[restic.ID]restic.ID
	destinationMappingsLoaded   bool
	visitedTrees                restic.AssociatedBlobSet
}

func (s *Store) observeCopy(event string) {
	if s != nil && s.observeCopySession != nil {
		s.observeCopySession(event)
	}
}

func (s *Store) openCopySession(ctx context.Context, destination *Store, connections uint) (*copySession, error) {
	sourceRepo, sourceCtx, unlockSource, err := s.openLockedRepositoryWithConnections(ctx, false, connections)
	if err != nil {
		return nil, err
	}
	destinationRepo, destinationCtx, unlockDestination, err := destination.openLockedRepositoryWithConnections(sourceCtx, false, connections)
	if err != nil {
		unlockSource()
		_ = sourceRepo.Close()
		return nil, err
	}
	cleanup := func() {
		unlockDestination()
		_ = destinationRepo.Close()
		unlockSource()
		_ = sourceRepo.Close()
	}
	if err := sourceRepo.LoadIndex(destinationCtx, restic.NoopTerminalCounterFactory); err != nil {
		cleanup()
		return nil, operationErrorForStores("load source repository index", err, s, destination)
	}
	s.observeCopy("source_index_loaded")
	if err := destinationRepo.LoadIndex(destinationCtx, restic.NoopTerminalCounterFactory); err != nil {
		cleanup()
		return nil, operationErrorForStores("load destination repository index", err, s, destination)
	}
	destination.observeCopy("target_index_loaded")
	return &copySession{
		source: s, destination: destination, sourceRepo: sourceRepo, destinationRepo: destinationRepo,
		ctx: destinationCtx, cleanup: cleanup, destinationMappings: make(map[restic.ID]restic.ID),
		visitedTrees: sourceRepo.NewAssociatedBlobSet(),
	}, nil
}

func (session *copySession) close() {
	if session != nil && session.cleanup != nil {
		session.cleanup()
		session.cleanup = nil
	}
}

func (session *copySession) loadDestinationMappings() error {
	if session.destinationMappingsLoaded {
		return nil
	}
	mappings, err := snapshotOriginalMappings(session.ctx, session.destinationRepo)
	if err != nil {
		return operationErrorForStores("list destination snapshots", err, session.source, session.destination)
	}
	session.destination.observeCopy("destination_originals_loaded")
	session.destinationMappings = mappings
	session.destinationMappingsLoaded = true
	return nil
}

func (session *copySession) copySnapshot(snapshot *data.Snapshot) (SnapshotMapping, bool, error) {
	if err := session.ctx.Err(); err != nil {
		return SnapshotMapping{}, false, err
	}
	directSourceID := *snapshot.ID()
	original := directSourceID
	if snapshot.Original != nil && !snapshot.Original.IsNull() {
		original = *snapshot.Original
	}
	if err := session.destinationRepo.WithBlobUploader(session.ctx, func(uploadCtx context.Context, uploader restic.BlobSaverWithAsync) error {
		return copySnapshotTree(uploadCtx, session.sourceRepo, session.destinationRepo, session.visitedTrees, *snapshot.Tree, uploader)
	}); err != nil {
		return SnapshotMapping{}, false, operationErrorForStores("copy snapshot data", err, session.source, session.destination)
	}
	if replacementID, exists := session.destinationMappings[original]; exists {
		return SnapshotMapping{SourceSnapshotID: directSourceID.String(), ReplacementSnapshotID: replacementID.String()}, true, nil
	}
	copied := *snapshot
	copied.Parent = nil
	copied.Original = &original
	newID, err := data.SaveSnapshot(session.ctx, session.destinationRepo, &copied)
	if err != nil {
		return SnapshotMapping{}, false, operationErrorForStores("save copied snapshot", err, session.source, session.destination)
	}
	session.destinationMappings[original] = newID
	return SnapshotMapping{SourceSnapshotID: directSourceID.String(), ReplacementSnapshotID: newID.String()}, false, nil
}

func (s *Store) Copy(ctx context.Context, destination *Store, request CopyRequest) (CopyResult, error) {
	if err := ctx.Err(); err != nil {
		return CopyResult{}, err
	}
	if destination == nil {
		return CopyResult{}, fmt.Errorf("copy destination is required")
	}
	if destination == s {
		return CopyResult{}, fmt.Errorf("copy destination must differ from source")
	}
	if request.WriteConcurrency > 64 {
		return CopyResult{}, fmt.Errorf("copy concurrency exceeds typed limit")
	}

	session, err := s.openCopySession(ctx, destination, request.WriteConcurrency)
	if err != nil {
		return CopyResult{}, err
	}
	defer session.close()
	if !request.CreateOnly {
		if err := session.loadDestinationMappings(); err != nil {
			return CopyResult{}, err
		}
	}
	snapshots, err := selectedSnapshots(session.ctx, session.sourceRepo, request.SnapshotIDs)
	if err != nil {
		return CopyResult{}, operationErrorForStores("select source snapshots", err, s, destination)
	}

	result := CopyResult{}
	for _, snapshot := range snapshots {
		mapping, skipped, copyErr := session.copySnapshot(snapshot)
		if copyErr != nil {
			return CopyResult{}, copyErr
		}
		if skipped {
			result.SkippedSnapshots++
			result.Mappings = append(result.Mappings, mapping)
			continue
		}
		result.CopiedSnapshots++
		result.SnapshotIDs = append(result.SnapshotIDs, mapping.ReplacementSnapshotID)
		result.Mappings = append(result.Mappings, mapping)
	}
	return result, session.ctx.Err()
}

// CopyAndVerifyBatch reuses one bounded source/destination repository session
// for multiple exact snapshots. Per-item verification failures retain their
// mapping so the caller can persist the external effect before rejecting
// publication. Session-level failures are returned separately.
func (s *Store) CopyAndVerifyBatch(ctx context.Context, destination *Store, request CopyVerifyBatchRequest) (CopyVerifyBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return CopyVerifyBatchResult{}, err
	}
	if destination == nil || destination == s || len(request.Items) == 0 || len(request.Items) > MaxCopyVerifyBatchItems || request.WriteConcurrency > 64 {
		return CopyVerifyBatchResult{}, fmt.Errorf("invalid copy verify batch request")
	}
	seen := make(map[string]struct{}, len(request.Items))
	for _, item := range request.Items {
		item.SnapshotID = strings.TrimSpace(item.SnapshotID)
		if _, exists := seen[item.SnapshotID]; exists {
			return CopyVerifyBatchResult{}, fmt.Errorf("copy verify batch contains duplicate snapshot")
		}
		seen[item.SnapshotID] = struct{}{}
		if err := validatePayloadVerificationRequest(VerifySnapshotPayloadRequest{
			SnapshotID: item.SnapshotID, ExpectedSize: item.ExpectedSize, ExpectedChecksum: item.ExpectedChecksum,
		}); err != nil {
			return CopyVerifyBatchResult{}, fmt.Errorf("invalid copy verify batch item")
		}
	}

	session, err := s.openCopySession(ctx, destination, request.WriteConcurrency)
	if err != nil {
		return CopyVerifyBatchResult{}, err
	}
	defer session.close()
	if !request.CreateOnly {
		if err := session.loadDestinationMappings(); err != nil {
			return CopyVerifyBatchResult{}, err
		}
	}
	result := CopyVerifyBatchResult{Items: make([]CopyVerifyItemResult, 0, len(request.Items))}
	for _, item := range request.Items {
		sourceID, _ := restic.ParseID(strings.TrimSpace(item.SnapshotID))
		snapshot, loadErr := data.LoadSnapshot(session.ctx, session.sourceRepo, sourceID)
		if loadErr != nil {
			result.Items = append(result.Items, CopyVerifyItemResult{Err: operationErrorForStores("load source snapshot", loadErr, s, destination)})
			continue
		}
		mapping, _, copyErr := session.copySnapshot(snapshot)
		if copyErr != nil {
			return result, copyErr
		}
		itemResult := CopyVerifyItemResult{Mapping: mapping}
		itemResult.Verification, itemResult.Err = destination.verifySnapshotPayloadInRepository(session.ctx, session.destinationRepo, VerifySnapshotPayloadRequest{
			SnapshotID: mapping.ReplacementSnapshotID, ExpectedSize: item.ExpectedSize, ExpectedChecksum: item.ExpectedChecksum,
		})
		result.Items = append(result.Items, itemResult)
	}
	return result, session.ctx.Err()
}

// ForgetPrune removes explicitly selected snapshots, or (for legacy callers)
// older repository snapshots while retaining KeepLast snapshots, then reclaims
// unreferenced repository data.
func (s *Store) ForgetPrune(ctx context.Context, request RetentionRequest) (RetentionResult, error) {
	if err := ctx.Err(); err != nil {
		return RetentionResult{}, err
	}
	if request.KeepLast == 0 && len(request.ForgetSnapshotIDs) == 0 {
		return RetentionResult{}, fmt.Errorf("retention keep last must be at least one")
	}
	protected := restic.NewIDSet()
	for _, rawID := range request.ProtectedSnapshotIDs {
		id, err := restic.ParseID(strings.TrimSpace(rawID))
		if err != nil {
			return RetentionResult{}, fmt.Errorf("protected snapshot ID is invalid")
		}
		protected.Insert(id)
	}
	requested := restic.NewIDSet()
	for _, rawID := range request.ForgetSnapshotIDs {
		id, err := restic.ParseID(strings.TrimSpace(rawID))
		if err != nil {
			return RetentionResult{}, fmt.Errorf("forget snapshot ID is invalid")
		}
		if protected.Has(id) {
			return RetentionResult{}, fmt.Errorf("forget snapshot ID is protected")
		}
		requested.Insert(id)
	}
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, true)
	if err != nil {
		return RetentionResult{}, err
	}
	defer unlock()
	defer func() { _ = repo.Close() }()

	snapshots, err := selectedSnapshots(lockedCtx, repo, nil)
	if err != nil {
		return RetentionResult{}, s.operationError("list snapshots for retention", err)
	}
	sort.Sort(snapshots)
	result := RetentionResult{KeptSnapshots: uint64(len(snapshots))}
	remove := restic.NewIDSet()
	if len(request.ForgetSnapshotIDs) > 0 {
		available := restic.NewIDSet()
		for _, snapshot := range snapshots {
			available.Insert(*snapshot.ID())
		}
		for id := range requested {
			if available.Has(id) {
				remove.Insert(id)
			}
		}
		// An exact purge is resumable: a missing requested snapshot means a
		// previous attempt completed the destructive step before the caller
		// persisted its application-level completion marker.
		result.ForgottenSnapshots = uint64(len(requested))
	} else {
		if uint64(len(snapshots)) <= request.KeepLast {
			return result, lockedCtx.Err()
		}
		for _, snapshot := range snapshots[request.KeepLast:] {
			if !protected.Has(*snapshot.ID()) {
				remove.Insert(*snapshot.ID())
			}
		}
	}
	if len(remove) == 0 {
		return result, lockedCtx.Err()
	}
	if err := restic.ParallelRemove(lockedCtx, repo, remove, restic.WriteableSnapshotFile, nil, restic.NoopCounter); err != nil {
		return RetentionResult{}, s.operationError("forget expired snapshots", err)
	}
	result.KeptSnapshots = uint64(len(snapshots)) - uint64(len(remove))
	if len(request.ForgetSnapshotIDs) == 0 {
		result.ForgottenSnapshots = uint64(len(remove))
	}
	if err := s.prune(lockedCtx, repo); err != nil {
		return RetentionResult{}, err
	}
	return result, lockedCtx.Err()
}

// Stats measures persisted Restic repository objects for one provider.
func (s *Store) Stats(ctx context.Context) (StatsResult, error) {
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		return StatsResult{}, err
	}
	defer unlock()
	defer func() { _ = repo.Close() }()

	result := StatsResult{}
	for _, fileType := range []restic.FileType{restic.PackFile, restic.SnapshotFile, restic.IndexFile, restic.KeyFile} {
		if err := repo.List(lockedCtx, fileType, func(_ restic.ID, size int64) error {
			if size < 0 {
				return fmt.Errorf("repository returned negative object size")
			}
			result.RepositoryBytes += uint64(size)
			if fileType == restic.SnapshotFile {
				result.SnapshotCount++
			}
			return nil
		}); err != nil {
			return StatsResult{}, s.operationError("measure repository", err)
		}
	}
	return result, nil
}

// Check performs an exclusive deep integrity check of repository indexes,
// packs, snapshots, trees, and every encrypted data blob. Callers are expected
// to bound this deliberately expensive operation with a context deadline.
func (s *Store) Check(ctx context.Context) error {
	repo, lockedCtx, unlock, err := s.openLockedRepositoryForIntegrity(ctx)
	if err != nil {
		return classifyIntegrityCheckError(ctx, err, IntegrityCodeOpenUnavailable)
	}
	defer unlock()
	defer func() { _ = repo.Close() }()

	check := checker.New(repo, false)
	if err := check.LoadSnapshots(lockedCtx, &data.SnapshotFilter{}, nil); err != nil {
		return classifyIntegrityCheckError(lockedCtx, err, IntegrityCodeCheckUnavailable)
	}
	hints, errs := check.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory)
	if len(hints) != 0 || len(errs) != 0 {
		return classifyIntegrityCheckError(lockedCtx, firstCheckError(hints, errs), IntegrityCodeCheckUnavailable)
	}
	packErrors := make(chan error)
	go check.Packs(lockedCtx, packErrors)
	var firstPackError error
	for checkErr := range packErrors {
		if firstPackError == nil {
			firstPackError = checkErr
		}
	}
	if firstPackError != nil {
		return classifyIntegrityCheckError(lockedCtx, firstPackError, IntegrityCodeCheckUnavailable)
	}
	structureErrors := make(chan error)
	go check.Structure(lockedCtx, restic.NoopCounter, structureErrors)
	var firstStructureError error
	for checkErr := range structureErrors {
		if firstStructureError == nil {
			firstStructureError = checkErr
		}
	}
	if firstStructureError != nil {
		return classifyIntegrityCheckError(lockedCtx, firstStructureError, IntegrityCodeCheckUnavailable)
	}
	dataErrors := make(chan error)
	go check.ReadPacks(lockedCtx, func(packs map[restic.ID]int64) map[restic.ID]int64 {
		return packs
	}, restic.NewNoopPrinter(), dataErrors)
	var firstDataError error
	for checkErr := range dataErrors {
		if firstDataError == nil {
			firstDataError = checkErr
		}
	}
	if firstDataError != nil {
		return classifyIntegrityCheckError(lockedCtx, firstDataError, IntegrityCodeCheckUnavailable)
	}
	if err := lockedCtx.Err(); err != nil {
		return classifyIntegrityCheckError(lockedCtx, err, IntegrityCodeCheckUnavailable)
	}
	return nil
}

func (s *Store) openLockedRepositoryForIntegrity(ctx context.Context) (*repository.Repository, context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	backendValue, err := s.backendWithConnections(ctx, false, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	repo, err := repository.New(backendValue, repository.Options{Compression: repository.CompressionFastest})
	if err != nil {
		_ = backendValue.Close()
		return nil, nil, nil, err
	}
	if err := repo.SearchKey(ctx, s.config.RepositoryPassword, maxRepositoryKeys, ""); err != nil {
		_ = repo.Close()
		return nil, nil, nil, err
	}
	unlock, lockedCtx, err := repository.LockRepo(ctx, repo, true, integrityLockRetryTimeout, func(string) {}, func(string, ...interface{}) {})
	if err != nil {
		_ = repo.Close()
		return nil, nil, nil, err
	}
	return repo, lockedCtx, unlock, nil
}

func classifyIntegrityCheckError(ctx context.Context, err error, fallback string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return newIntegrityError(IntegrityCodeTimeout, true, false)
	}
	if repository.IsAlreadyLocked(err) {
		return newIntegrityError(IntegrityCodeLocked, true, false)
	}
	if verifiedIntegrityCorruption(err) {
		return newIntegrityError(IntegrityCodeCorrupt, false, true)
	}
	if fallback != IntegrityCodeOpenUnavailable {
		fallback = IntegrityCodeCheckUnavailable
	}
	return newIntegrityError(fallback, true, false)
}

func verifiedIntegrityCorruption(err error) bool {
	var packMetadata *repository.ErrPackMetadata
	if errors.As(err, &packMetadata) {
		return true
	}
	var packData *repository.ErrPackData
	if errors.As(err, &packData) {
		return true
	}
	var structure *checker.Error
	if errors.As(err, &structure) {
		return true
	}
	var tree *checker.TreeError
	if errors.As(err, &tree) {
		for _, nested := range tree.Errors {
			if verifiedIntegrityCorruption(nested) {
				return true
			}
		}
	}
	return false
}

// RepairCorruptPacks uses Restic's repository repair implementation to
// salvage intact encrypted blobs and remove pack/index entries that failed a
// full data read. A subsequent Copy from a verified repository restores the
// missing blobs while preserving immutable snapshot identity.
func (s *Store) RepairCorruptPacks(ctx context.Context) error {
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, true)
	if err != nil {
		return err
	}
	defer unlock()
	defer func() { _ = repo.Close() }()

	check := checker.New(repo, false)
	if err := check.LoadSnapshots(lockedCtx, &data.SnapshotFilter{}, nil); err != nil {
		return s.operationError("load snapshots for pack repair", err)
	}
	hints, indexErrors := check.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory)
	if len(hints) != 0 || len(indexErrors) != 0 {
		return s.operationError("load repository index for pack repair", firstCheckError(hints, indexErrors))
	}
	dataErrors := make(chan error)
	go check.ReadPacks(lockedCtx, func(packs map[restic.ID]int64) map[restic.ID]int64 {
		return packs
	}, restic.NewNoopPrinter(), dataErrors)
	damagedPacks := restic.NewIDSet()
	var unsupportedError error
	for checkErr := range dataErrors {
		var packError *repository.ErrPackData
		if errors.As(checkErr, &packError) {
			damagedPacks.Insert(packError.PackID)
			continue
		}
		if unsupportedError == nil {
			unsupportedError = checkErr
		}
	}
	if unsupportedError != nil {
		return s.operationError("identify damaged repository packs", unsupportedError)
	}
	if len(damagedPacks) == 0 {
		return lockedCtx.Err()
	}
	if err := repository.RepairPacks(lockedCtx, repo, damagedPacks, restic.NewNoopPrinter()); err != nil {
		return s.operationError("repair damaged repository packs", err)
	}
	return lockedCtx.Err()
}

func (s *Store) openRepository(ctx context.Context) (*repository.Repository, error) {
	return s.openRepositoryWithConnections(ctx, 0)
}

func (s *Store) openRepositoryWithConnections(ctx context.Context, connections uint) (*repository.Repository, error) {
	be, err := s.backendWithConnections(ctx, false, connections)
	if err != nil {
		return nil, s.operationError("open provider", err)
	}
	repo, err := repository.New(be, repository.Options{Compression: repository.CompressionFastest})
	if err != nil {
		return nil, s.operationError("open repository", err)
	}
	if err := repo.SearchKey(ctx, s.config.RepositoryPassword, maxRepositoryKeys, ""); err != nil {
		_ = repo.Close()
		return nil, s.operationError("unlock repository", err)
	}
	if s.config.CacheDirectory != "" {
		repositoryCache, err := backendcache.New(repo.Config().ID, s.config.CacheDirectory)
		if err != nil {
			_ = repo.Close()
			return nil, errors.New("resticstore repository metadata cache unavailable")
		}
		repo.UseCache(repositoryCache, func(string, ...interface{}) {})
	}
	return repo, nil
}

func (s *Store) openLockedRepository(ctx context.Context, exclusive bool) (*repository.Repository, context.Context, func(), error) {
	return s.openLockedRepositoryWithConnections(ctx, exclusive, 0)
}

func (s *Store) openLockedRepositoryWithConnections(ctx context.Context, exclusive bool, connections uint) (*repository.Repository, context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	repo, err := s.openRepositoryWithConnections(ctx, connections)
	if err != nil {
		return nil, nil, nil, err
	}
	unlock, lockedCtx, err := repository.LockRepo(ctx, repo, exclusive, 0, func(string) {}, func(string, ...interface{}) {})
	if err != nil {
		_ = repo.Close()
		return nil, nil, nil, s.operationError("acquire repository lock", err)
	}
	return repo, lockedCtx, unlock, nil
}

func (s *Store) backend(ctx context.Context, create bool) (backend.Backend, error) {
	return s.backendWithConnections(ctx, create, 0)
}

func (s *Store) backendWithConnections(ctx context.Context, create bool, connections uint) (backend.Backend, error) {
	if connections > 64 {
		return nil, fmt.Errorf("provider connection concurrency exceeds typed limit")
	}
	switch s.config.Provider.Kind {
	case ProviderLocal:
		config := local.NewConfig()
		config.Path = s.config.Provider.Local.Path
		if connections > 0 {
			config.Connections = connections
		}
		if create {
			return local.Create(ctx, config, func(string, ...interface{}) {})
		}
		return local.Open(ctx, config, func(string, ...interface{}) {})
	case ProviderS3:
		provider := s.config.Provider.S3
		config := s3.NewConfig()
		config.Endpoint = provider.Endpoint
		config.UseHTTP = provider.UseHTTP
		config.Bucket = provider.Bucket
		config.Prefix = provider.Prefix
		config.Region = provider.Region
		config.BucketLookup = "path"
		config.KeyID = provider.AccessKey
		config.Secret = options.NewSecretString(provider.SecretKey)
		if connections > 0 {
			config.Connections = connections
		}
		if create {
			return s3.Create(ctx, config, provider.Transport, func(string, ...interface{}) {})
		}
		return s3.Open(ctx, config, provider.Transport, func(string, ...interface{}) {})
	default:
		return nil, fmt.Errorf("unsupported provider kind")
	}
}

func (s *Store) operationError(operation string, err error) error {
	return operationErrorForStores(operation, err, s)
}

func operationErrorForStores(operation string, err error, stores ...*Store) error {
	message := err.Error()
	for _, store := range stores {
		if store == nil {
			continue
		}
		for _, secret := range store.secrets() {
			if secret != "" {
				message = strings.ReplaceAll(message, secret, "[REDACTED]")
			}
		}
	}
	return fmt.Errorf("resticstore %s: %s", operation, message)
}

func (s *Store) secrets() []string {
	secrets := []string{s.config.RepositoryPassword}
	if s.config.Provider.S3 != nil {
		secrets = append(secrets, s.config.Provider.S3.AccessKey, s.config.Provider.S3.SecretKey)
	}
	return secrets
}

func cloneConfig(config Config) Config {
	clone := config
	if config.Provider.Local != nil {
		local := *config.Provider.Local
		clone.Provider.Local = &local
	}
	if config.Provider.S3 != nil {
		s3 := *config.Provider.S3
		clone.Provider.S3 = &s3
	}
	return clone
}

func firstCheckError(hints, errs []error) error {
	if len(errs) != 0 {
		return errs[0]
	}
	if len(hints) != 0 {
		return hints[0]
	}
	return fmt.Errorf("repository check failed")
}

func acquireInitializationPermit(ctx context.Context, permit <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-permit:
		return nil
	}
}

func selectedSnapshots(ctx context.Context, repo *repository.Repository, ids []string) (data.Snapshots, error) {
	snapshots := data.Snapshots{}
	filter := data.SnapshotFilter{}
	err := filter.FindAll(ctx, repo, repo, ids, func(_ string, snapshot *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		snapshots = append(snapshots, snapshot)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshots, ctx.Err()
}

func snapshotOriginalMappings(ctx context.Context, repo *repository.Repository) (map[restic.ID]restic.ID, error) {
	mappings := make(map[restic.ID]restic.ID)
	err := data.ForAllSnapshots(ctx, repo, repo, nil, func(id restic.ID, snapshot *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		original := id
		if snapshot.Original != nil && !snapshot.Original.IsNull() {
			original = *snapshot.Original
		}
		mappings[original] = id
		return nil
	})
	if err != nil {
		return nil, err
	}
	return mappings, ctx.Err()
}

func copySnapshotTree(ctx context.Context, source *repository.Repository, destination *repository.Repository, visitedTrees restic.AssociatedBlobSet, rootTree restic.ID, uploader restic.BlobSaverWithAsync) error {
	copyBlobs := source.NewAssociatedBlobSet()
	packs := restic.NewIDSet()
	var mutex sync.Mutex
	enqueue := func(handle restic.BlobHandle) {
		mutex.Lock()
		defer mutex.Unlock()
		if _, exists := destination.LookupBlobSize(handle); exists {
			return
		}
		copyBlobs.Insert(handle)
		for _, packedBlob := range source.LookupBlob(handle) {
			packs.Insert(packedBlob.PackID())
		}
	}

	err := data.StreamTrees(ctx, source, restic.IDs{rootTree}, restic.NoopCounter, func(treeID restic.ID) bool {
		handle := restic.BlobHandle{ID: treeID, Type: restic.TreeBlob}
		mutex.Lock()
		seen := visitedTrees.Has(handle)
		visitedTrees.Insert(handle)
		mutex.Unlock()
		return seen
	}, func(treeID restic.ID, err error, nodes data.TreeNodeIterator) error {
		if err != nil {
			return fmt.Errorf("load tree %s: %w", treeID.Str(), err)
		}
		enqueue(restic.BlobHandle{ID: treeID, Type: restic.TreeBlob})
		for item := range nodes {
			if item.Error != nil {
				return item.Error
			}
			for _, blobID := range item.Node.Content {
				enqueue(restic.BlobHandle{ID: blobID, Type: restic.DataBlob})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return repository.CopyBlobs(ctx, source, destination, uploader, packs, copyBlobs, restic.NoopCounter, func(string, ...interface{}) {})
}

func (s *Store) prune(ctx context.Context, repo *repository.Repository) error {
	if err := repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory); err != nil {
		return s.operationError("load repository index for prune", err)
	}
	plan, err := repository.PlanPrune(ctx, repository.PruneOptions{
		MaxUnusedBytes: func(uint64) uint64 { return 0 },
		MaxRepackBytes: math.MaxUint64,
	}, repo, func(ctx context.Context, repository restic.Repository, usedBlobs restic.FindBlobSet) error {
		return findUsedBlobs(ctx, repository, usedBlobs)
	}, restic.NewNoopPrinter())
	if err != nil {
		return s.operationError("plan repository prune", err)
	}
	if err := plan.Execute(ctx, restic.NewNoopPrinter()); err != nil {
		return s.operationError("prune repository", err)
	}
	return ctx.Err()
}

func findUsedBlobs(ctx context.Context, repo restic.Repository, usedBlobs restic.FindBlobSet) error {
	var trees restic.IDs
	if err := data.ForAllSnapshots(ctx, repo, repo, nil, func(_ restic.ID, snapshot *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		if snapshot.Tree == nil {
			return fmt.Errorf("snapshot has no tree")
		}
		trees = append(trees, *snapshot.Tree)
		return nil
	}); err != nil {
		return err
	}
	return data.FindUsedBlobs(ctx, repo, trees, usedBlobs, restic.NoopCounter)
}

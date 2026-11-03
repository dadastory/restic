package resticstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

const uploadCheckpointTag = "flashyun-upload-checkpoint"

// UploadCheckpoint identifies a durable native prefix. It is acknowledged only
// after the caller commits it in its authoritative upload metadata.
type UploadCheckpoint struct {
	SnapshotID     string
	Offset         int64
	DataAddedBytes uint64
	Checksum       string // SHA-256 of the exact durable prefix, encoded as sha256:hex.
}

// UploadAppendRequest supplies only bytes after the exact acknowledged prefix.
// Commit must fence the expected head, generation and writable lifecycle. It is
// called after packs, index and snapshot are durable, under a shared native
// repository lock. A rejected/ambiguous candidate remains rooted for cleanup.
// Source remains caller-owned; cancellation must also interrupt blocked reads.
type UploadAppendRequest struct {
	UploadID           string
	PreviousSnapshotID string
	Offset             int64
	ExpectedSize       int64
	CheckpointBytes    int64
	Source             io.Reader
	Commit             func(context.Context, UploadCheckpoint) error
}

// UploadFinalizeRequest adopts one complete checkpoint as a normal final
// snapshot. ReconcileExisting is reserved for uncertain durable-effect replay.
type UploadFinalizeRequest struct {
	UploadID          string
	SnapshotID        string
	ExpectedSize      int64
	ExpectedChecksum  string
	IdempotencyKey    string
	ReconcileExisting bool
}

// AppendUpload writes native content-defined chunks without a whole-file copy.
// Periodic commits occur at natural chunk boundaries, at most one native chunk
// beyond CheckpointBytes. An arbitrary request EOF is replayed on the next call
// from only the last blob, so request boundaries do not change final blob IDs.
// On any failure the returned checkpoint is the last successfully committed one.
func (s *Store) AppendUpload(ctx context.Context, request UploadAppendRequest) (UploadCheckpoint, error) {
	committed := UploadCheckpoint{SnapshotID: request.PreviousSnapshotID, Offset: request.Offset}
	ownerTag, err := uploadOwnerTag(request.UploadID)
	if err != nil || request.Offset < 0 || request.ExpectedSize < request.Offset || request.CheckpointBytes <= 0 || request.Source == nil || request.Commit == nil || (request.PreviousSnapshotID == "" && request.Offset != 0) {
		return committed, fmt.Errorf("invalid native upload append request")
	}
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		return committed, err
	}
	defer func() { unlock(); _ = repo.Close() }()
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return committed, s.operationError("load upload index", err)
	}
	factory := repo.ChunkerFactory()
	node := &data.Node{Name: "payload", Type: data.NodeTypeFile, Mode: 0o600, ModTime: time.Now().UTC(), Content: []restic.ID{}}
	digest := sha256.New()
	var tail []byte
	if request.PreviousSnapshotID != "" {
		snapshot, previous, err := loadUploadCheckpoint(lockedCtx, repo, request.PreviousSnapshotID, ownerTag)
		if err != nil {
			return committed, s.operationError("load upload checkpoint", err)
		}
		if previous.Size != uint64(request.Offset) {
			return committed, fmt.Errorf("native upload prefix size mismatch")
		}
		digest, err = restoreUploadHash(snapshot, previous.Size)
		if err != nil {
			return committed, err
		}
		committed.Checksum = "sha256:" + hex.EncodeToString(digest.Sum(nil))
		node = previous
		if snapshot.Summary != nil {
			committed.DataAddedBytes = snapshot.Summary.DataAdded
		}
		if len(node.Content) > 0 {
			last := node.Content[len(node.Content)-1]
			tail, err = repo.LoadBlob(lockedCtx, restic.BlobHandle{Type: restic.DataBlob, ID: last}, nil)
			if err != nil {
				return committed, s.operationError("restore upload tail", err)
			}
			if len(tail) == 0 || len(tail) > factory.MaxChunkSize() || uint64(len(tail)) > node.Size {
				return committed, fmt.Errorf("invalid native upload tail")
			}
			node.Content = node.Content[:len(node.Content)-1]
			node.Size -= uint64(len(tail))
		}
	}
	stream := &uploadChunkReader{ctx: lockedCtx, source: io.MultiReader(bytes.NewReader(tail), request.Source), chunker: factory.NewChunker(), readBuf: make([]byte, 512<<10), chunk: make([]byte, 0, factory.MaxChunkSize())}
	added := committed.DataAddedBytes
	for {
		var treeID restic.ID
		var sourceErr error
		changed := false
		err := repo.WithBlobUploader(lockedCtx, func(writeCtx context.Context, saver restic.BlobSaverWithAsync) error {
			for {
				chunk, readErr := stream.next()
				if len(chunk) > 0 {
					if uint64(len(chunk)) > uint64(request.ExpectedSize)-node.Size {
						return fmt.Errorf("native upload exceeds expected size")
					}
					id, _, physical, err := saver.SaveBlob(writeCtx, restic.DataBlob, chunk, restic.ID{}, false)
					if err != nil {
						return err
					}
					added += uint64(physical)
					// Tail replay restores native chunk boundaries, but the saved hash
					// already includes those bytes. Hash only this request's suffix.
					skip := min(int64(len(chunk)), max(int64(0), request.Offset-int64(node.Size)))
					_, _ = digest.Write(chunk[skip:])
					node.Content = append(node.Content, id)
					node.Size += uint64(len(chunk))
				}
				if node.Size == uint64(request.ExpectedSize) {
					// Final CAS may publish staged immediately. A natural chunk split
					// is not source EOF: reject trailing bytes/errors before that CAS.
					if readErr == nil {
						var extra []byte
						extra, readErr = stream.next()
						if len(extra) > 0 {
							return fmt.Errorf("native upload exceeds expected size")
						}
					}
					if !errors.Is(readErr, io.EOF) {
						if readErr == nil {
							return io.ErrNoProgress
						}
						return readErr
					}
				}
				if readErr != nil {
					sourceErr = readErr
					break
				}
				if int64(node.Size)-committed.Offset >= request.CheckpointBytes {
					break
				}
			}
			changed = int64(node.Size) > committed.Offset || committed.SnapshotID == ""
			if !changed {
				return nil
			}
			builder := data.NewTreeJSONBuilder()
			if err := builder.AddNode(node); err != nil {
				return err
			}
			tree, err := builder.Finalize()
			if err != nil {
				return err
			}
			var physical int
			treeID, _, physical, err = saver.SaveBlob(writeCtx, restic.TreeBlob, tree, restic.ID{}, false)
			added += uint64(physical)
			return err
		})
		if err != nil {
			return committed, s.operationError("write upload checkpoint", err)
		}
		if changed {
			now := time.Now().UTC()
			hashTag, err := uploadHashStateTag(digest, node.Size)
			if err != nil {
				return committed, err
			}
			snapshot := &data.Snapshot{Time: now, Tree: &treeID, Paths: []string{"/payload"}, Tags: []string{uploadCheckpointTag, ownerTag, hashTag}, ProgramVersion: "flashyun-resticstore", Summary: &data.SnapshotSummary{BackupStart: node.ModTime, BackupEnd: now, TotalFilesProcessed: 1, TotalBytesProcessed: node.Size, DataAdded: added}}
			id, err := data.SaveSnapshot(lockedCtx, repo, snapshot)
			if err != nil {
				return committed, s.operationError("save upload checkpoint", err)
			}
			checksum, err := uploadCheckpointChecksum(snapshot, node, int64(node.Size), "")
			if err != nil {
				return committed, err
			}
			candidate := UploadCheckpoint{SnapshotID: id.String(), Offset: int64(node.Size), DataAddedBytes: added, Checksum: checksum}
			if err := request.Commit(lockedCtx, candidate); err != nil {
				return committed, err
			}
			previousID := committed.SnapshotID
			committed = candidate
			if previousID != "" {
				// Only the exact, validated predecessor is obsolete after successful
				// external CAS. This is snapshot metadata removal, never pack GC.
				// Existing readers/writers hold shared locks, excluding native prune.
				id, err := restic.ParseID(previousID)
				if err != nil {
					return committed, err
				}
				if err := repo.RemoveUnpacked(lockedCtx, restic.WriteableSnapshotFile, id); err != nil {
					return committed, s.operationError("retire superseded upload checkpoint", err)
				}
			}
		}
		if sourceErr != nil {
			if errors.Is(sourceErr, io.EOF) {
				return committed, nil
			}
			return committed, sourceErr
		}
	}
}

// uploadChunkReader preserves source errors even when a Read returns bytes with
// the error. Its buffers never exceed one native chunk plus a bounded read slab.
type uploadChunkReader struct {
	ctx                     context.Context
	source                  io.Reader
	chunker                 restic.Chunker
	readBuf, pending, chunk []byte
	terminal                error
}

func (r *uploadChunkReader) next() ([]byte, error) {
	r.chunk = r.chunk[:0]
	for emptyReads := 0; ; {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		if len(r.pending) > 0 {
			split := r.chunker.NextSplitPoint(r.pending)
			if split >= 0 {
				r.chunk = append(r.chunk, r.pending[:split]...)
				r.pending = r.pending[split:]
				return r.chunk, nil
			}
			r.chunk = append(r.chunk, r.pending...)
			r.pending = nil
		}
		if r.terminal != nil {
			return r.chunk, r.terminal
		}
		n, err := r.source.Read(r.readBuf)
		if n < 0 || n > len(r.readBuf) {
			return nil, fmt.Errorf("invalid upload source read")
		}
		r.pending, r.terminal = r.readBuf[:n], err
		if n == 0 && err == nil {
			emptyReads++
			if emptyReads >= 100 {
				return r.chunk, io.ErrNoProgress
			}
		} else {
			emptyReads = 0
		}
	}
}

func uploadOwnerTag(uploadID string) (string, error) {
	if uploadID == "" || strings.TrimSpace(uploadID) != uploadID {
		return "", fmt.Errorf("invalid native upload identity")
	}
	digest := sha256.Sum256([]byte(uploadID))
	return "flashyun-upload:" + hex.EncodeToString(digest[:]), nil
}

func uploadFinalTag(ownerTag string) string {
	return "flashyun-upload-final:" + strings.TrimPrefix(ownerTag, "flashyun-upload:")
}

func loadUploadCheckpoint(ctx context.Context, repo *repository.Repository, snapshotID, ownerTag string) (*data.Snapshot, *data.Node, error) {
	id, err := restic.ParseID(snapshotID)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := data.LoadSnapshot(ctx, repo, id)
	if err != nil {
		return nil, nil, err
	}
	if !snapshot.HasTags([]string{uploadCheckpointTag, ownerTag}) || snapshot.Tree == nil {
		return nil, nil, fmt.Errorf("snapshot is not the requested upload checkpoint")
	}
	node, err := loadUploadPayload(ctx, repo, snapshot)
	return snapshot, node, err
}

func loadUploadPayload(ctx context.Context, repo *repository.Repository, snapshot *data.Snapshot) (*data.Node, error) {
	if snapshot.Tree == nil {
		return nil, fmt.Errorf("missing upload payload tree")
	}
	tree, err := data.LoadTree(ctx, repo, *snapshot.Tree)
	if err != nil {
		return nil, err
	}
	var node *data.Node
	for item := range tree {
		if item.Error != nil {
			return nil, item.Error
		}
		if node != nil || item.Node.Name != "payload" || item.Node.Type != data.NodeTypeFile {
			return nil, fmt.Errorf("invalid upload payload tree")
		}
		node = item.Node
	}
	if node == nil {
		return nil, fmt.Errorf("missing upload payload")
	}
	var size uint64
	for _, id := range node.Content {
		length, ok := repo.LookupBlobSize(restic.BlobHandle{Type: restic.DataBlob, ID: id})
		if !ok || length == 0 || int64(length) > int64(repo.ChunkerFactory().MaxChunkSize()) || uint64(length) > node.Size-size {
			return nil, fmt.Errorf("invalid upload prefix blob")
		}
		size += uint64(length)
	}
	if size != node.Size {
		return nil, fmt.Errorf("invalid upload prefix size")
	}
	return node, nil
}

// OpenUpload reads an exact checkpoint. Close releases its native read lock and
// repository, so callers must close it even when they stop before EOF.
func (s *Store) OpenUpload(ctx context.Context, uploadID, snapshotID string) (io.ReadCloser, int64, error) {
	ownerTag, err := uploadOwnerTag(uploadID)
	if err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithCancel(ctx)
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		cancel()
		return nil, 0, err
	}
	release := func() error { unlock(); return repo.Close() }
	fail := func(err error) (io.ReadCloser, int64, error) {
		cancel()
		_ = release()
		return nil, 0, s.operationError("open upload prefix", err)
	}
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return fail(err)
	}
	_, node, err := loadUploadCheckpoint(lockedCtx, repo, snapshotID, ownerTag)
	if err != nil {
		return fail(err)
	}
	payload, err := s.newPayloadReader(lockedCtx, repo, node, int64(node.Size), cancel, release)
	if err != nil {
		return fail(err)
	}
	return &uploadRangeReader{SectionReader: io.NewSectionReader(payload, 0, payload.Size()), payload: payload}, payload.Size(), nil
}

type uploadRangeReader struct {
	*io.SectionReader
	payload *SnapshotPayload
}

func (reader *uploadRangeReader) Close() error { return reader.payload.Close() }

// UploadChecksum returns the server-computed checksum for one complete, owned
// checkpoint using authenticated snapshot/tree/hash metadata. It neither reads
// payload blobs nor creates a final snapshot or other durable upload effect.
func (s *Store) UploadChecksum(ctx context.Context, uploadID, snapshotID string, expectedSize int64) (string, error) {
	ownerTag, err := uploadOwnerTag(uploadID)
	if err != nil || validatePayloadVerificationRequest(VerifySnapshotPayloadRequest{SnapshotID: snapshotID, ExpectedSize: expectedSize}) != nil {
		return "", fmt.Errorf("invalid upload checksum request")
	}
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		return "", err
	}
	defer func() { unlock(); _ = repo.Close() }()
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return "", s.operationError("load upload index", err)
	}
	snapshot, node, err := loadUploadCheckpoint(lockedCtx, repo, snapshotID, ownerTag)
	if err != nil {
		return "", s.operationError("load upload checksum", err)
	}
	return uploadCheckpointChecksum(snapshot, node, expectedSize, "")
}

// FinalizeUpload validates the persisted server-computed SHA-256 and native tree
// metadata without rescanning payload bytes, then reuses its tree/blobs. Native
// reads and integrity maintenance detect subsequent provider corruption. Final
// roots deliberately have no checkpoint owner tag.
func (s *Store) FinalizeUpload(ctx context.Context, request UploadFinalizeRequest) (SnapshotResult, error) {
	ownerTag, err := uploadOwnerTag(request.UploadID)
	operationTag, operationErr := readerOperationTag(request.IdempotencyKey)
	if err != nil || operationErr != nil || validatePayloadVerificationRequest(VerifySnapshotPayloadRequest{SnapshotID: request.SnapshotID, ExpectedSize: request.ExpectedSize, ExpectedChecksum: request.ExpectedChecksum}) != nil {
		return SnapshotResult{}, fmt.Errorf("invalid upload finalization request")
	}
	repo, lockedCtx, unlock, err := s.openLockedRepository(ctx, false)
	if err != nil {
		return SnapshotResult{}, err
	}
	defer func() { unlock(); _ = repo.Close() }()
	if err := repo.LoadIndex(lockedCtx, restic.NoopTerminalCounterFactory); err != nil {
		return SnapshotResult{}, s.operationError("load upload index", err)
	}
	finalTag, sourceTag := uploadFinalTag(ownerTag), "flashyun-upload-source:"+request.SnapshotID
	if request.ReconcileExisting {
		results, err := findSnapshotsByOperationTags(lockedCtx, repo, map[string]struct{}{operationTag: {}})
		if err != nil {
			return SnapshotResult{}, s.operationError("reconcile upload snapshot", err)
		}
		if result, found := results[operationTag]; found {
			id, err := restic.ParseID(result.ID)
			if err != nil {
				return SnapshotResult{}, err
			}
			snapshot, err := data.LoadSnapshot(lockedCtx, repo, id)
			if err != nil {
				return SnapshotResult{}, s.operationError("load finalized upload", err)
			}
			if !snapshot.HasTags([]string{finalTag, sourceTag}) {
				return SnapshotResult{}, fmt.Errorf("upload operation identity mismatch")
			}
			node, err := loadUploadPayload(lockedCtx, repo, snapshot)
			if err != nil {
				return SnapshotResult{}, err
			}
			value, err := uploadTagValue(snapshot, uploadChecksumTagPrefix)
			if err != nil {
				return SnapshotResult{}, err
			}
			result.Checksum = "sha256:" + value
			if err := validateUploadChecksum(snapshot, node, request.ExpectedSize, result.Checksum, request.ExpectedChecksum); err != nil {
				return SnapshotResult{}, err
			}
			return result, nil
		}
	}
	snapshot, node, err := loadUploadCheckpoint(lockedCtx, repo, request.SnapshotID, ownerTag)
	if err != nil {
		return SnapshotResult{}, s.operationError("load complete upload", err)
	}
	checksum, err := uploadCheckpointChecksum(snapshot, node, request.ExpectedSize, request.ExpectedChecksum)
	if err != nil {
		return SnapshotResult{}, err
	}
	snapshot.Time = time.Now().UTC()
	snapshot.Tags = []string{finalTag, sourceTag, operationTag, uploadChecksumTagPrefix + strings.TrimPrefix(checksum, "sha256:")}
	id, err := data.SaveSnapshot(lockedCtx, repo, snapshot)
	if err != nil {
		return SnapshotResult{}, s.operationError("save finalized upload", err)
	}
	result := SnapshotResult{ID: id.String(), ProcessedBytes: node.Size, Checksum: checksum, StartedAt: node.ModTime, CompletedAt: snapshot.Time}
	if snapshot.Summary != nil {
		result.DataAddedBytes = snapshot.Summary.DataAdded
		result.StartedAt = snapshot.Summary.BackupStart
		result.CompletedAt = snapshot.Summary.BackupEnd
	}
	return result, nil
}

// CleanupUpload removes this upload's checkpoint roots only, including candidates
// left by failed commits. The caller must first fence all writers/finalization.
// Native exclusive prune retains every blob reachable from any surviving root.
func (s *Store) CleanupUpload(ctx context.Context, uploadID string) error {
	return s.cleanupUploadRoots(ctx, uploadID, false, nil)
}

// DiscardUploadFinals reclaims unpublished native finalization/copy effects.
// The domain caller must fence finalization and supply every catalog-referenced
// snapshot in this repository. Absence of a publication event is not a fence.
func (s *Store) DiscardUploadFinals(ctx context.Context, uploadID string, protectedSnapshotIDs []string) error {
	protected := restic.NewIDSet()
	for _, value := range protectedSnapshotIDs {
		id, err := restic.ParseID(value)
		if err != nil {
			return err
		}
		protected.Insert(id)
	}
	return s.cleanupUploadRoots(ctx, uploadID, true, protected)
}

func (s *Store) cleanupUploadRoots(ctx context.Context, uploadID string, finals bool, protected restic.IDSet) error {
	ownerTag, err := uploadOwnerTag(uploadID)
	if err != nil {
		return err
	}
	repo, err := s.openRepository(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = repo.Close() }()
	// A process can exit after committing a checkpoint but before removing its
	// shared lock. Delegate stale/dead-process detection to Restic itself so
	// repeated durable cleanup can converge; never force-remove active locks.
	if _, err := repository.RemoveStaleLocks(ctx, repo); err != nil {
		return s.operationError("remove stale upload repository locks", err)
	}
	unlock, lockedCtx, err := repository.LockRepo(ctx, repo, true, 0, func(string) {}, func(string, ...interface{}) {})
	if err != nil {
		return s.operationError("lock upload repository for cleanup", err)
	}
	defer unlock()
	remove := restic.NewIDSet()
	if err := data.ForAllSnapshots(lockedCtx, repo, repo, nil, func(id restic.ID, snapshot *data.Snapshot, err error) error {
		if err != nil {
			return err
		}
		owned := snapshot.HasTags([]string{uploadCheckpointTag, ownerTag})
		if finals {
			owned = snapshot.HasTags([]string{uploadFinalTag(ownerTag)})
		}
		if owned && !protected.Has(id) {
			remove.Insert(id)
		}
		return nil
	}); err != nil {
		return s.operationError("list upload checkpoints", err)
	}
	if err := restic.ParallelRemove(lockedCtx, repo, remove, restic.WriteableSnapshotFile, nil, restic.NoopCounter); err != nil {
		return s.operationError("remove upload checkpoints", err)
	}
	return s.prune(lockedCtx, repo)
}

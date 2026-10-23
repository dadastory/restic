package resticstore

import (
	"crypto/sha256"
	"encoding"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"strings"

	"github.com/restic/restic/internal/data"
)

const (
	uploadHashStateTagPrefix = "flashyun-upload-sha256-state-v1:"
	uploadChecksumTagPrefix  = "flashyun-upload-sha256:"
)

// The fixed Go crypto/sha256 implementation owns the binary state format. It
// includes a partial plaintext block, so it belongs only inside Restic's
// authenticated, encrypted snapshot and must never be exposed as client state.
func uploadHashStateTag(digest hash.Hash, size uint64) (string, error) {
	marshaler, ok := digest.(encoding.BinaryMarshaler)
	if !ok {
		return "", fmt.Errorf("SHA-256 state serialization unavailable")
	}
	state, err := marshaler.MarshalBinary()
	if err != nil {
		return "", err
	}
	return uploadHashStateTagPrefix + strconv.FormatUint(size, 10) + ":" + base64.RawStdEncoding.EncodeToString(state), nil
}

func restoreUploadHash(snapshot *data.Snapshot, size uint64) (hash.Hash, error) {
	value, err := uploadTagValue(snapshot, uploadHashStateTagPrefix)
	if err != nil {
		return nil, err
	}
	encodedSize, encodedState, ok := strings.Cut(value, ":")
	storedSize, sizeErr := strconv.ParseUint(encodedSize, 10, 64)
	if !ok || sizeErr != nil || storedSize != size {
		return nil, fmt.Errorf("upload SHA-256 prefix size mismatch")
	}
	state, err := base64.RawStdEncoding.DecodeString(encodedState)
	if err != nil {
		return nil, fmt.Errorf("invalid upload SHA-256 state: %w", err)
	}
	digest := sha256.New()
	unmarshaler, ok := digest.(encoding.BinaryUnmarshaler)
	if !ok {
		return nil, fmt.Errorf("SHA-256 state restoration unavailable")
	}
	if err := unmarshaler.UnmarshalBinary(state); err != nil {
		return nil, fmt.Errorf("invalid upload SHA-256 state: %w", err)
	}
	return digest, nil
}

func uploadTagValue(snapshot *data.Snapshot, prefix string) (string, error) {
	var value string
	found := false
	for _, tag := range snapshot.Tags {
		if !strings.HasPrefix(tag, prefix) {
			continue
		}
		if found {
			return "", fmt.Errorf("duplicate upload integrity metadata")
		}
		value, found = strings.TrimPrefix(tag, prefix), true
	}
	if !found || value == "" {
		return "", fmt.Errorf("missing upload integrity metadata")
	}
	return value, nil
}

func validateUploadChecksum(snapshot *data.Snapshot, node *data.Node, size int64, checksum, expected string) error {
	if node.Size != uint64(size) || snapshot.Summary == nil || snapshot.Summary.TotalBytesProcessed != node.Size {
		return fmt.Errorf("complete upload size mismatch")
	}
	digest, err := hex.DecodeString(strings.TrimPrefix(checksum, "sha256:"))
	if !strings.HasPrefix(checksum, "sha256:") || err != nil || len(digest) != sha256.Size {
		return fmt.Errorf("invalid upload checksum metadata")
	}
	if expected != "" && !strings.EqualFold(checksum, expected) {
		return fmt.Errorf("complete upload checksum mismatch")
	}
	return nil
}

func uploadCheckpointChecksum(snapshot *data.Snapshot, node *data.Node, size int64, expected string) (string, error) {
	digest, err := restoreUploadHash(snapshot, node.Size)
	if err != nil {
		return "", err
	}
	checksum := "sha256:" + hex.EncodeToString(digest.Sum(nil))
	if err := validateUploadChecksum(snapshot, node, size, checksum, expected); err != nil {
		return "", err
	}
	return checksum, nil
}

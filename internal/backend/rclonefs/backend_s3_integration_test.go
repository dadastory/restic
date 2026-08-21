//go:build integration

package rclonefs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/stretchr/testify/require"
)

func TestS3BackendInterruptedPublicationCancellationAndConcurrency(t *testing.T) {
	endpoint := os.Getenv("FLASHYUN_RCLONE_S3_ENDPOINT")
	accessKey := os.Getenv("FLASHYUN_RCLONE_S3_ACCESS_KEY")
	secretKey := os.Getenv("FLASHYUN_RCLONE_S3_SECRET_KEY")
	bucket := os.Getenv("FLASHYUN_RCLONE_S3_BUCKET")
	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skip("FlashYun rclone S3 integration environment is not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root := fmt.Sprintf("%s/rclone-backend/%d", bucket, time.Now().UTC().UnixNano())
	storage, err := Open(ctx, ProviderSnapshot{
		Backend: "s3",
		Root:    root,
		Options: map[string]string{
			"provider":          "Other",
			"endpoint":          "http://" + endpoint,
			"access_key_id":     accessKey,
			"secret_access_key": secretKey,
			"force_path_style":  "true",
			"no_check_bucket":   "false",
			"upload_cutoff":     "5Mi",
			"chunk_size":        "5Mi",
		},
	}, true, 8)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		require.NoError(t, storage.Delete(cleanupCtx))
		require.NoError(t, storage.Close())
	})

	config := backend.Handle{Type: backend.ConfigFile}
	require.NoError(t, storage.Save(ctx, config, backend.NewByteReader([]byte("complete"), nil)))

	interrupted := &failingRewindReader{payload: bytes.Repeat([]byte("x"), 6<<20), failAfter: (5 << 20) + 1024}
	require.Error(t, storage.Save(ctx, config, interrupted))
	require.Equal(t, []byte("complete"), loadS3TestObject(t, ctx, storage, config, 0, 0))

	missing := backend.Handle{Type: backend.PackFile, Name: "interrupted"}
	interrupted = &failingRewindReader{payload: bytes.Repeat([]byte("y"), 6<<20), failAfter: (5 << 20) + 1024}
	require.Error(t, storage.Save(ctx, missing, interrupted))
	_, err = storage.Stat(ctx, missing)
	require.Error(t, err)
	require.True(t, storage.IsNotExist(err))

	cancelled, stop := context.WithCancel(ctx)
	stop()
	cancelledHandle := backend.Handle{Type: backend.PackFile, Name: "cancelled"}
	require.Error(t, storage.Save(cancelled, cancelledHandle, backend.NewByteReader(bytes.Repeat([]byte("z"), 6<<20), nil)))
	_, err = storage.Stat(ctx, cancelledHandle)
	require.Error(t, err)
	require.True(t, storage.IsNotExist(err))

	const objectCount = 12
	var wait sync.WaitGroup
	errorsByIndex := make([]error, objectCount)
	for index := 0; index < objectCount; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			payload := bytes.Repeat([]byte{byte(index)}, 128<<10)
			handle := backend.Handle{Type: backend.PackFile, Name: fmt.Sprintf("concurrent-%02d", index)}
			errorsByIndex[index] = storage.Save(ctx, handle, backend.NewByteReader(payload, nil))
		}()
	}
	wait.Wait()
	for index, saveErr := range errorsByIndex {
		require.NoErrorf(t, saveErr, "concurrent object %d", index)
	}

	ranged := loadS3TestObject(t, ctx, storage, backend.Handle{Type: backend.PackFile, Name: "concurrent-07"}, 2048, 4096)
	require.Equal(t, bytes.Repeat([]byte{7}, 2048), ranged)
}

func loadS3TestObject(t *testing.T, ctx context.Context, storage *Backend, handle backend.Handle, length int, offset int64) []byte {
	t.Helper()
	var content []byte
	require.NoError(t, storage.Load(ctx, handle, length, offset, func(input io.Reader) error {
		var err error
		content, err = io.ReadAll(input)
		return err
	}))
	return content
}

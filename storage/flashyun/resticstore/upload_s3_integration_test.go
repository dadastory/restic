//go:build integration

package resticstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestUploadNativeIsolatedS3RestartAndCleanup(t *testing.T) {
	endpoint, bucket := os.Getenv("FLASHYUN_RESTICSTORE_S3_ENDPOINT"), os.Getenv("FLASHYUN_RESTICSTORE_S3_UPLOAD_BUCKET")
	access, secret := os.Getenv("FLASHYUN_RESTICSTORE_S3_ACCESS_KEY"), os.Getenv("FLASHYUN_RESTICSTORE_S3_SECRET_KEY")
	if endpoint == "" || access == "" || secret == "" || !strings.HasPrefix(bucket, "flashyun-stream-probe-") {
		t.Skip("requires explicitly isolated flashyun-stream-probe S3 bucket and credentials")
	}
	store, err := New(Config{Provider: testS3Provider(endpoint, true, bucket, "native-upload/"+time.Now().UTC().Format("20060102150405.000000000"), "", access, secret), RepositoryPassword: "native-upload-isolated-test"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	if err := store.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	payload := uploadTestPayload(32<<20 + 113)
	started := time.Now()
	var commits int
	interrupted, err := store.AppendUpload(ctx, UploadAppendRequest{UploadID: "s3-native-upload", ExpectedSize: int64(len(payload)), CheckpointBytes: 4 << 20, Source: io.MultiReader(bytes.NewReader(payload[:13<<20+71]), uploadErrorReader{io.ErrUnexpectedEOF}), Commit: func(context.Context, UploadCheckpoint) error { commits++; return nil }})
	if !errors.Is(err, io.ErrUnexpectedEOF) || commits < 2 {
		t.Fatalf("interruption commits=%d error=%v", commits, err)
	}
	reopened, err := New(store.config)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := reopened.AppendUpload(ctx, UploadAppendRequest{UploadID: "s3-native-upload", PreviousSnapshotID: interrupted.SnapshotID, Offset: interrupted.Offset, ExpectedSize: int64(len(payload)), CheckpointBytes: 4 << 20, Source: bytes.NewReader(payload[interrupted.Offset:]), Commit: acceptUpload})
	if err != nil {
		t.Fatal(err)
	}
	assertUploadBytes(t, reopened, "s3-native-upload", resumed, payload)
	final, err := reopened.FinalizeUpload(ctx, UploadFinalizeRequest{UploadID: "s3-native-upload", SnapshotID: resumed.SnapshotID, ExpectedSize: int64(len(payload)), IdempotencyKey: "native-s3-final"})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	stats, err := reopened.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	continuous, err := reopened.SnapshotReader(ctx, readerBatchRequests([][]byte{payload}, "native-s3-continuous")[0])
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(uploadContentIDs(t, reopened, resumed.SnapshotID), uploadContentIDs(t, reopened, continuous.ID)) {
		t.Fatal("S3 suffix continuation changed native blob identities")
	}
	if err := reopened.CleanupUpload(ctx, "s3-native-upload"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{final.ID, continuous.ID} {
		if _, err := reopened.VerifySnapshotPayload(ctx, VerifySnapshotPayloadRequest{SnapshotID: id, ExpectedSize: int64(len(payload)), ExpectedChecksum: final.Checksum}); err != nil {
			t.Fatal(err)
		}
	}
	if err := reopened.Check(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("native S3 bytes=%d interrupted_offset=%d resumed_source_bytes=%d intermediate_commits=%d repository_bytes_before_comparison=%d elapsed=%s", len(payload), interrupted.Offset, int64(len(payload))-interrupted.Offset, commits, stats.RepositoryBytes, elapsed)
}

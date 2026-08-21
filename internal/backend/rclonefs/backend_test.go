package rclonefs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/restic/restic/internal/backend"
	"github.com/stretchr/testify/require"
)

func TestLocalBackendContract(t *testing.T) {
	ctx := context.Background()
	be, err := Open(ctx, ProviderSnapshot{Backend: "local", Root: t.TempDir()}, true, 4)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, be.Close()) })

	pack := backend.Handle{Type: backend.PackFile, Name: "aabbcc"}
	snapshot := backend.Handle{Type: backend.SnapshotFile, Name: "snapshot-1"}
	require.NoError(t, be.Save(ctx, pack, backend.NewByteReader([]byte("0123456789"), nil)))
	require.NoError(t, be.Save(ctx, snapshot, backend.NewByteReader([]byte("snapshot"), nil)))

	stat, err := be.Stat(ctx, pack)
	require.NoError(t, err)
	require.Equal(t, int64(10), stat.Size)

	var ranged []byte
	require.NoError(t, be.Load(ctx, pack, 4, 3, func(reader io.Reader) error {
		var readErr error
		ranged, readErr = io.ReadAll(reader)
		return readErr
	}))
	require.Equal(t, []byte("3456"), ranged)

	err = be.Load(ctx, pack, 4, 8, func(io.Reader) error { return nil })
	require.Error(t, err)
	require.True(t, be.IsPermanentError(err))

	var packs []string
	require.NoError(t, be.List(ctx, backend.PackFile, func(info backend.FileInfo) error {
		packs = append(packs, info.Name)
		return nil
	}))
	require.Equal(t, []string{"aabbcc"}, packs)

	var snapshots []string
	require.NoError(t, be.List(ctx, backend.SnapshotFile, func(info backend.FileInfo) error {
		snapshots = append(snapshots, info.Name)
		return nil
	}))
	require.Equal(t, []string{"snapshot-1"}, snapshots)

	require.NoError(t, be.Remove(ctx, snapshot))
	_, err = be.Stat(ctx, snapshot)
	require.Error(t, err)
	require.True(t, be.IsNotExist(err))

	require.NoError(t, be.Delete(ctx))
	_, err = be.Stat(ctx, pack)
	require.Error(t, err)
	require.True(t, be.IsNotExist(err))
}

func TestInterruptedLocalSaveNeverReplacesFinalObject(t *testing.T) {
	ctx := context.Background()
	be, err := Open(ctx, ProviderSnapshot{Backend: "local", Root: t.TempDir()}, true, 2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, be.Close()) })

	handle := backend.Handle{Type: backend.ConfigFile}
	require.NoError(t, be.Save(ctx, handle, backend.NewByteReader([]byte("complete"), nil)))

	reader := &failingRewindReader{payload: []byte("replacement"), failAfter: 4}
	require.Error(t, be.Save(ctx, handle, reader))

	var content []byte
	require.NoError(t, be.Load(ctx, handle, 0, 0, func(input io.Reader) error {
		var readErr error
		content, readErr = io.ReadAll(input)
		return readErr
	}))
	require.Equal(t, []byte("complete"), content)
}

func TestCatalogRejectsUnknownBackendAndRuntimeMutation(t *testing.T) {
	_, err := Open(context.Background(), ProviderSnapshot{Backend: "union", Root: "ignored"}, false, 1)
	require.ErrorIs(t, err, ErrBackendNotAdmitted)

	mapper := newImmutableMapper(map[string]string{"region": "local"})
	value, ok := mapper.Get("region")
	require.True(t, ok)
	require.Equal(t, "local", value)
	mapper.Set("token", "secret")
	require.True(t, mapper.Mutated())
	require.Equal(t, []string{"token"}, mapper.MutatedKeys())
	_, ok = mapper.Get("token")
	require.False(t, ok)
}

type failingRewindReader struct {
	payload   []byte
	failAfter int
	reader    *bytes.Reader
}

func (reader *failingRewindReader) Read(buffer []byte) (int, error) {
	if reader.reader == nil {
		reader.reader = bytes.NewReader(reader.payload)
	}
	consumed := len(reader.payload) - reader.reader.Len()
	if consumed >= reader.failAfter {
		return 0, errors.New("injected read failure")
	}
	if len(buffer) > reader.failAfter-consumed {
		buffer = buffer[:reader.failAfter-consumed]
	}
	return reader.reader.Read(buffer)
}

func (reader *failingRewindReader) Rewind() error {
	reader.reader = bytes.NewReader(reader.payload)
	return nil
}

func (reader *failingRewindReader) Length() int64 { return int64(len(reader.payload)) }
func (reader *failingRewindReader) Hash() []byte  { return nil }

func TestAdmittedBackendNamesAreStable(t *testing.T) {
	require.True(t, slices.Equal(AdmittedBackends(), []string{"azureblob", "b2", "gcs", "local", "s3", "sftp", "webdav"}))
}

func TestEveryAdmittedBackendMapsToARegisteredRcloneBackend(t *testing.T) {
	for _, backendName := range AdmittedBackends() {
		capability, ok := admittedCatalog[backendName]
		require.True(t, ok, backendName)
		require.NotEmpty(t, capability.RcloneBackend, backendName)
		_, err := fs.Find(capability.RcloneBackend)
		require.NoError(t, err, backendName)
	}
}

func TestBackendConstructionContextStopsFollowingRequestAfterConstruction(t *testing.T) {
	requestContext, cancelRequest := context.WithCancel(context.Background())
	backendContext, finishConstruction, cancelBackend := newBackendConstructionContext(requestContext)
	finishConstruction()
	cancelRequest()
	require.NoError(t, backendContext.Err(), "a completed shared runtime must not inherit later request cancellation")
	cancelBackend()
	require.ErrorIs(t, backendContext.Err(), context.Canceled)
}

func TestBackendConstructionContextHonorsCancellationDuringConstruction(t *testing.T) {
	requestContext, cancelRequest := context.WithCancel(context.Background())
	backendContext, finishConstruction, cancelBackend := newBackendConstructionContext(requestContext)
	t.Cleanup(cancelBackend)
	cancelRequest()
	require.Eventually(t, func() bool {
		return errors.Is(backendContext.Err(), context.Canceled)
	}, time.Second, time.Millisecond)
	finishConstruction()
}

func TestCommonBackendSnapshotsRejectUnapprovedCredentialSources(t *testing.T) {
	tests := []struct {
		name     string
		snapshot ProviderSnapshot
	}{
		{name: "azure environment auth", snapshot: ProviderSnapshot{Backend: "azureblob", Root: "container/workspaces/a", Options: map[string]string{"account": "account", "key": "key", "env_auth": "true"}}},
		{name: "gcs credential file", snapshot: ProviderSnapshot{Backend: "gcs", Root: "bucket/workspaces/a", Options: map[string]string{"service_account_file": "/secret.json"}}},
		{name: "sftp key file", snapshot: ProviderSnapshot{Backend: "sftp", Root: "/workspaces/a", Options: map[string]string{"host": "storage.example", "user": "user", "key_file": "/key"}}},
		{name: "webdav credential command", snapshot: ProviderSnapshot{Backend: "webdav", Root: "workspaces/a", Options: map[string]string{"url": "https://dav.example", "bearer_token_command": "token"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, ValidateSnapshot(test.snapshot))
		})
	}
}

func TestLocalProviderNeutralProbeUsesAdapterContract(t *testing.T) {
	root := t.TempDir()
	snapshot := ProviderSnapshot{Backend: "local", Root: root}
	objects := make([]ProbeObject, 0, 2)
	stages := ProbeSnapshotObserved(context.Background(), "", snapshot, time.Second, nil, func(object ProbeObject) error {
		objects = append(objects, object)
		return nil
	})
	codes := make([]string, 0, len(stages))
	for _, stage := range stages {
		codes = append(codes, stage.Code)
		require.NoError(t, stage.Err, stage.Code)
	}
	require.Equal(t, []string{"construct", "write", "range", "checksum", "delete", "absence"}, codes)
	require.Len(t, objects, 2)
	require.True(t, objects[0].Present)
	require.False(t, objects[1].Present)
	require.NoError(t, CleanupProbeObject(context.Background(), snapshot, objects[0].Name))
}

package archiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/fs"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/test"
	"golang.org/x/sync/errgroup"
)

type unexpectedEOFReader struct{ calls int }

func (r *unexpectedEOFReader) Read([]byte) (int, error) {
	r.calls++
	if r.calls > 2 {
		return 0, errors.New("unexpected EOF was retried without progress")
	}
	return 0, io.ErrUnexpectedEOF
}

func TestReadNextChunkStopsOnUnexpectedEOFWithoutProgress(t *testing.T) {
	reader := &unexpectedEOFReader{}
	state := fileChunkState{readBuf: make([]byte, 16)}
	chunker := repository.TestRepository(t).ChunkerFactory().NewChunker()
	_, err := state.readNextChunk(reader, chunker, nil)
	if !errors.Is(err, io.ErrUnexpectedEOF) || reader.calls != 1 {
		t.Fatalf("readNextChunk = %v after %d reads, want unexpected EOF after one read", err, reader.calls)
	}
}

func TestReadNextChunkPreservesShortFinalBuffers(t *testing.T) {
	for _, payload := range [][]byte{nil, []byte("short"), bytes.Repeat([]byte("x"), 35)} {
		state := fileChunkState{readBuf: make([]byte, 16)}
		chunker := repository.TestRepository(t).ChunkerFactory().NewChunker()
		reader := bytes.NewReader(payload)
		var got []byte
		for {
			chunk, err := state.readNextChunk(reader, chunker, nil)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, chunk...)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("readNextChunk = %q, want %q", got, payload)
		}
	}
}

func createTestFiles(t testing.TB, num int) (files []string) {
	tempdir := test.TempDir(t)

	for i := range num {
		filename := fmt.Sprintf("testfile-%d", i)
		err := os.WriteFile(filepath.Join(tempdir, filename), []byte(filename), 0600)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, filepath.Join(tempdir, filename))
	}

	return files
}

func startFileSaver(ctx context.Context, t testing.TB, _ fs.FS) (*fileSaver, *mockSaver, context.Context, *errgroup.Group) {
	wg, ctx := errgroup.WithContext(ctx)

	workers := uint(runtime.NumCPU())
	chunkerFactory := repository.TestRepository(t).ChunkerFactory()

	saver := &mockSaver{saved: make(map[string]int)}
	s := newFileSaver(ctx, wg, saver, chunkerFactory, workers)
	s.NodeFromFileInfo = func(snPath, filename string, meta toNoder, ignoreXattrListError bool) (*data.Node, error) {
		return meta.ToNode(ignoreXattrListError, t.Logf)
	}

	return s, saver, ctx, wg
}

func TestFileSaver(t *testing.T) {
	ctx := t.Context()

	startFn := func() {}
	completeReadingFn := func() {}
	completeFn := func(*data.Node, ItemStats) {}

	files := createTestFiles(t, 15)
	testFs := fs.NewLocal()
	s, saver, ctx, wg := startFileSaver(ctx, t, testFs)

	var results []futureNode

	for _, filename := range files {
		f, err := testFs.OpenFile(filename, os.O_RDONLY, false)
		if err != nil {
			t.Fatal(err)
		}

		ff := s.Save(ctx, filename, filename, f, startFn, completeReadingFn, completeFn)
		results = append(results, ff)
	}

	for _, file := range results {
		fnr := file.take(ctx)
		if fnr.err != nil {
			t.Errorf("unable to save file: %v", fnr.err)
		}
	}

	test.Assert(t, len(saver.saved) == len(files), "expected %d saved files, got %d", len(files), len(saver.saved))

	s.TriggerShutdown()

	err := wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

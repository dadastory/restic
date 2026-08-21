package rclonefs

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

const (
	defaultRuntimePoolSize         = 64
	defaultRuntimeConnections      = 5
	maximumRuntimeConnections      = 64
	defaultConcurrentConstructions = 16
)

var errInvalidRuntimeConnections = errors.New("invalid rclone runtime connection ceiling")

type runtimeOpener func(context.Context, ProviderSnapshot, bool, uint) (*Backend, error)

type runtimeEntry struct {
	backend    *Backend
	references int
	lastUsed   time.Time
}

type runtimeConstruction struct {
	done  chan struct{}
	entry *runtimeEntry
	err   error
}

type runtimePool struct {
	mu           sync.Mutex
	entries      map[string]*runtimeEntry
	constructing map[string]*runtimeConstruction
	maximumIdle  int
	admission    *semaphore.Weighted
	open         runtimeOpener
	closed       bool
}

var sharedRuntimePool = newRuntimePool(defaultRuntimePoolSize, defaultConcurrentConstructions)

func newRuntimePool(maximumIdle, maximumConstructions int) *runtimePool {
	if maximumIdle < 1 {
		maximumIdle = 1
	}
	if maximumConstructions < 1 {
		maximumConstructions = 1
	}
	return &runtimePool{
		entries:      make(map[string]*runtimeEntry),
		constructing: make(map[string]*runtimeConstruction),
		maximumIdle:  maximumIdle,
		admission:    semaphore.NewWeighted(int64(maximumConstructions)),
		open:         Open,
	}
}

// Acquire returns a lease for one immutable provider revision. The identity is
// supplied by Storage and must contain no secret values.
func Acquire(ctx context.Context, identity string, snapshot ProviderSnapshot, create bool, connections uint) (*Backend, error) {
	if identity == "" {
		return Open(ctx, snapshot, create, connections)
	}
	return sharedRuntimePool.acquire(ctx, identity, snapshot, create, connections)
}

func (pool *runtimePool) acquire(ctx context.Context, identity string, snapshot ProviderSnapshot, create bool, connections uint) (*Backend, error) {
	key, normalizedConnections, err := canonicalRuntimeKey(identity, snapshot, connections)
	if err != nil {
		return nil, err
	}
	for {
		pool.mu.Lock()
		if pool.closed {
			pool.mu.Unlock()
			return nil, errors.New("rclone runtime pool is closed")
		}
		if entry := pool.entries[key]; entry != nil {
			entry.references++
			entry.lastUsed = time.Now()
			lease := pool.lease(key, entry)
			pool.mu.Unlock()
			return lease, nil
		}
		if construction := pool.constructing[key]; construction != nil {
			done := construction.done
			pool.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				if construction.err != nil {
					return nil, construction.err
				}
				continue
			}
		}
		construction := &runtimeConstruction{done: make(chan struct{})}
		pool.constructing[key] = construction
		pool.mu.Unlock()

		if err := pool.admission.Acquire(ctx, 1); err != nil {
			pool.completeConstruction(key, construction, nil, err)
			return nil, err
		}
		created, err := pool.open(ctx, snapshot, create, normalizedConnections)
		pool.admission.Release(1)
		pool.mu.Lock()
		delete(pool.constructing, key)
		construction.err = err
		if err == nil {
			construction.entry = &runtimeEntry{backend: created, references: 1, lastUsed: time.Now()}
			pool.entries[key] = construction.entry
		}
		close(construction.done)
		if err != nil {
			pool.mu.Unlock()
			return nil, err
		}
		lease := pool.lease(key, construction.entry)
		pool.evictIdleLocked()
		pool.mu.Unlock()
		return lease, nil
	}
}

func (pool *runtimePool) completeConstruction(key string, construction *runtimeConstruction, entry *runtimeEntry, err error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.constructing[key] != construction {
		return
	}
	delete(pool.constructing, key)
	construction.entry = entry
	construction.err = err
	close(construction.done)
}

func canonicalRuntimeKey(identity string, snapshot ProviderSnapshot, connections uint) (string, uint, error) {
	normalizedConnections, err := normalizeRuntimeConnections(connections)
	if err != nil {
		return "", 0, err
	}
	digest := sha256.New()
	writeDigestSegment(digest, snapshot.Backend)
	writeDigestSegment(digest, snapshot.Root)
	keys := make([]string, 0, len(snapshot.Options))
	for key := range snapshot.Options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeDigestSegment(digest, key)
		writeDigestSegment(digest, snapshot.Options[key])
	}
	return identity + ":" + strconv.FormatUint(uint64(normalizedConnections), 10) + ":" + fmt.Sprintf("%x", digest.Sum(nil)), normalizedConnections, nil
}

func writeDigestSegment(digest interface{ Write([]byte) (int, error) }, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write([]byte(value))
}

func normalizeRuntimeConnections(connections uint) (uint, error) {
	if connections == 0 {
		return defaultRuntimeConnections, nil
	}
	if connections > maximumRuntimeConnections {
		return 0, errInvalidRuntimeConnections
	}
	return connections, nil
}

func (pool *runtimePool) lease(key string, entry *runtimeEntry) *Backend {
	return &Backend{
		filesystem:   entry.backend.filesystem,
		layout:       entry.backend.layout,
		capabilities: entry.backend.capabilities,
		connections:  entry.backend.connections,
		release: func() error {
			pool.mu.Lock()
			defer pool.mu.Unlock()
			current := pool.entries[key]
			if current == nil || current != entry || current.references < 1 {
				return errors.New("invalid rclone runtime lease")
			}
			current.references--
			current.lastUsed = time.Now()
			pool.evictIdleLocked()
			return nil
		},
	}
}

func (pool *runtimePool) evictIdleLocked() {
	type idleEntry struct {
		key      string
		lastUsed time.Time
	}
	idle := make([]idleEntry, 0, len(pool.entries))
	for key, entry := range pool.entries {
		if entry.references == 0 {
			idle = append(idle, idleEntry{key: key, lastUsed: entry.lastUsed})
		}
	}
	if len(idle) <= pool.maximumIdle {
		return
	}
	sort.Slice(idle, func(left, right int) bool { return idle[left].lastUsed.Before(idle[right].lastUsed) })
	for _, candidate := range idle[:len(idle)-pool.maximumIdle] {
		entry := pool.entries[candidate.key]
		delete(pool.entries, candidate.key)
		_ = entry.backend.shutdown()
	}
}

func (pool *runtimePool) close() error {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	pool.closed = true
	var result error
	for key, entry := range pool.entries {
		if entry.references != 0 {
			result = errors.Join(result, errors.New("rclone runtime lease is still active"))
			continue
		}
		result = errors.Join(result, entry.backend.shutdown())
		delete(pool.entries, key)
	}
	return result
}

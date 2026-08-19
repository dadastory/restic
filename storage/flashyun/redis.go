package flashyun

import (
	"context"
	"errors"
	"time"

	"github.com/bsm/redislock"
)

// RedisLeaseConfig configures a distributed lease for one logical file.
type RedisLeaseConfig struct {
	TTL           time.Duration
	RetryStrategy redislock.RetryStrategy
}

// RedisLeaseLocker acquires Redis leases for individual FlashYun file keys.
// It never acquires a process-wide or repository-wide application lock.
type RedisLeaseLocker struct {
	client *redislock.Client
	ttl    time.Duration
	retry  redislock.RetryStrategy
}

// NewRedisLeaseLocker constructs a Redis-backed lease locker. Callers bound
// acquisition by passing a deadline on the context supplied to Mutate.
func NewRedisLeaseLocker(client redislock.RedisClient, config RedisLeaseConfig) (*RedisLeaseLocker, error) {
	if client == nil {
		return nil, errors.New("flashyun storage Redis client is required")
	}
	if config.TTL <= 0 {
		return nil, errors.New("flashyun storage Redis lease TTL must be positive")
	}

	return &RedisLeaseLocker{
		client: redislock.New(client),
		ttl:    config.TTL,
		retry:  config.RetryStrategy,
	}, nil
}

// LeaseTTL returns the actual lifetime passed to Redis when acquiring and
// refreshing this locker's leases.
func (l *RedisLeaseLocker) LeaseTTL() time.Duration {
	return l.ttl
}

// Acquire obtains a token-owned lease for exactly resource.
func (l *RedisLeaseLocker) Acquire(ctx context.Context, resource Resource) (Lease, error) {
	key, err := resource.LockKey()
	if err != nil {
		return nil, err
	}

	lock, err := l.client.Obtain(ctx, key, l.ttl, &redislock.Options{RetryStrategy: l.retry})
	if err != nil {
		return nil, err
	}
	return &redisLease{lock: lock, ttl: l.ttl}, nil
}

type redisLease struct {
	lock *redislock.Lock
	ttl  time.Duration
}

func (l *redisLease) Refresh(ctx context.Context) error {
	return l.lock.Refresh(ctx, l.ttl, nil)
}

func (l *redisLease) Release(ctx context.Context) error {
	return l.lock.Release(ctx)
}

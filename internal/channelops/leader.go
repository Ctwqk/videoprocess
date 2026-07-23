package channelops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	leaderServiceName     = "channelops-go"
	leaderAdvisoryLockKey = int64(0x43484f50534c4452)
)

var (
	ErrLeaderAuthorityUnavailable = errors.New("leader authority unavailable")
	ErrLeaderAuthorityLost        = errors.New("leader authority lost")
)

type LeaderAuthority struct {
	ServiceName string
	HolderID    string
	Epoch       int64
	AcquiredAt  time.Time
	HeartbeatAt time.Time
}

type leaderState struct {
	mu         sync.RWMutex
	configured bool
	authority  *LeaderAuthority
}

func (s *leaderState) configure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configured = true
}

func (s *leaderState) publish(authority LeaderAuthority) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configured = true
	s.authority = cloneLeaderAuthority(authority)
}

func (s *leaderState) snapshot() (bool, *LeaderAuthority) {
	if s == nil {
		return false, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.configured, cloneLeaderAuthorityPointer(s.authority)
}

func (s *leaderState) refresh(expected LeaderAuthority, acquiredAt time.Time, heartbeatAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !sameLeaderAuthority(s.authority, expected) {
		return
	}
	authority := *s.authority
	authority.AcquiredAt = acquiredAt
	authority.HeartbeatAt = heartbeatAt
	s.authority = &authority
}

func (s *leaderState) clear(expected LeaderAuthority) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sameLeaderAuthority(s.authority, expected) {
		s.authority = nil
	}
}

func cloneLeaderAuthority(authority LeaderAuthority) *LeaderAuthority {
	cloned := authority
	return &cloned
}

func cloneLeaderAuthorityPointer(authority *LeaderAuthority) *LeaderAuthority {
	if authority == nil {
		return nil
	}
	return cloneLeaderAuthority(*authority)
}

func sameLeaderAuthority(current *LeaderAuthority, expected LeaderAuthority) bool {
	return current != nil &&
		current.ServiceName == expected.ServiceName &&
		current.HolderID == expected.HolderID &&
		current.Epoch == expected.Epoch
}

type LeaderLease struct {
	mu        sync.Mutex
	conn      *pgxpool.Conn
	state     *leaderState
	released  bool
	Authority LeaderAuthority
}

func (s *Store) TryAcquireLeader(
	ctx context.Context,
	holderID string,
	now time.Time,
) (*LeaderLease, bool, error) {
	if strings.TrimSpace(holderID) == "" {
		return nil, false, errors.New("leader holder id must not be blank")
	}
	if s == nil || s.Pool == nil {
		return nil, false, errors.New("channelops store pool is not configured")
	}
	if s.leadership == nil {
		return nil, false, errors.New("channelops leadership state is not initialized")
	}
	s.leadership.configure()

	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}

	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderAdvisoryLockKey).Scan(&locked); err != nil {
		return nil, false, errors.Join(err, discardLeaderConn(conn))
	}
	if !locked {
		conn.Release()
		return nil, false, nil
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, false, errors.Join(err, unlockAndReleaseLeaderConn(ctx, conn))
	}

	now = now.UTC()
	var epoch int64
	err = tx.QueryRow(ctx, `
		INSERT INTO channelops_leader_epochs (
			service_name, epoch, holder_id, acquired_at, heartbeat_at, released_at
		) VALUES ('channelops-go', 1, $1, $2, $2, NULL)
		ON CONFLICT (service_name) DO UPDATE
		SET epoch = channelops_leader_epochs.epoch + 1,
		    holder_id = EXCLUDED.holder_id,
		    acquired_at = EXCLUDED.acquired_at,
		    heartbeat_at = EXCLUDED.heartbeat_at,
		    released_at = NULL
		RETURNING epoch
	`, holderID, now).Scan(&epoch)
	if err != nil {
		rollbackErr := tx.Rollback(ctx)
		return nil, false, errors.Join(err, rollbackErr, unlockAndReleaseLeaderConn(ctx, conn))
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, errors.Join(err, unlockAndReleaseLeaderConn(ctx, conn))
	}

	authority := LeaderAuthority{
		ServiceName: leaderServiceName,
		HolderID:    holderID,
		Epoch:       epoch,
		AcquiredAt:  now,
		HeartbeatAt: now,
	}
	lease := &LeaderLease{
		conn:      conn,
		state:     s.leadership,
		Authority: authority,
	}
	s.leadership.publish(authority)
	return lease, true, nil
}

func (l *LeaderLease) Heartbeat(ctx context.Context, now time.Time) error {
	if l == nil {
		return ErrLeaderAuthorityUnavailable
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.conn == nil {
		return ErrLeaderAuthorityLost
	}

	authority := l.Authority
	now = now.UTC()
	var acquiredAt time.Time
	err := l.conn.QueryRow(ctx, `
		UPDATE channelops_leader_epochs
		SET heartbeat_at = $3
		WHERE service_name = $1 AND holder_id = $2 AND epoch = $4
		RETURNING acquired_at
	`, authority.ServiceName, authority.HolderID, now, authority.Epoch).Scan(&acquiredAt)
	if err != nil {
		l.state.clear(authority)
		releaseErr := l.releaseConnectionLocked(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.Join(ErrLeaderAuthorityLost, releaseErr)
		}
		return errors.Join(err, releaseErr)
	}

	l.Authority.AcquiredAt = acquiredAt
	l.Authority.HeartbeatAt = now
	l.state.refresh(authority, acquiredAt, now)
	return nil
}

func (l *LeaderLease) Release(ctx context.Context, now time.Time) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released || l.conn == nil {
		return nil
	}

	authority := l.Authority
	tag, updateErr := l.conn.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET released_at = $4
		WHERE service_name = $1 AND holder_id = $2 AND epoch = $3
		  AND released_at IS NULL
	`, authority.ServiceName, authority.HolderID, authority.Epoch, now.UTC())
	if updateErr == nil && tag.RowsAffected() == 0 {
		updateErr = ErrLeaderAuthorityLost
	}
	l.state.clear(authority)
	return errors.Join(updateErr, l.releaseConnectionLocked(ctx))
}

func (l *LeaderLease) releaseConnectionLocked(ctx context.Context) error {
	if l.released || l.conn == nil {
		return nil
	}
	conn := l.conn
	l.conn = nil
	l.released = true
	return unlockAndReleaseLeaderConn(ctx, conn)
}

func unlockAndReleaseLeaderConn(ctx context.Context, conn *pgxpool.Conn) error {
	if conn == nil {
		return nil
	}

	var unlocked bool
	unlockErr := conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, leaderAdvisoryLockKey).Scan(&unlocked)
	if unlockErr == nil && !unlocked {
		unlockErr = errors.New("channelops leader advisory lock was not held")
	}
	if unlockErr != nil {
		closeErr := conn.Conn().Close(context.Background())
		unlockErr = errors.Join(unlockErr, closeErr)
	}
	conn.Release()
	return unlockErr
}

func discardLeaderConn(conn *pgxpool.Conn) error {
	if conn == nil {
		return nil
	}
	closeErr := conn.Conn().Close(context.Background())
	conn.Release()
	return closeErr
}

func (s *Store) WithLeaderExecutionFence(
	ctx context.Context,
	dispatch func(*Store) error,
) error {
	return s.withExecutionTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.assertLeaderAuthority(ctx, tx, true); err != nil {
			return err
		}
		return dispatch(s.withExecutionDB(tx, s.executionChannelID))
	})
}

func (s *Store) assertLeaderAuthority(
	ctx context.Context,
	db dbExecutor,
	required bool,
) error {
	var configured bool
	var authority *LeaderAuthority
	if s != nil {
		configured, authority = s.leadership.snapshot()
	}
	if !configured {
		if required {
			return ErrLeaderAuthorityUnavailable
		}
		return nil
	}
	if authority == nil {
		return ErrLeaderAuthorityUnavailable
	}

	var acquiredAt time.Time
	var heartbeatAt time.Time
	err := db.QueryRow(ctx, `
		SELECT acquired_at, heartbeat_at
		FROM channelops_leader_epochs
		WHERE service_name = $1 AND holder_id = $2 AND epoch = $3
		  AND released_at IS NULL
		FOR SHARE
	`, authority.ServiceName, authority.HolderID, authority.Epoch).Scan(&acquiredAt, &heartbeatAt)
	if errors.Is(err, pgx.ErrNoRows) {
		s.leadership.clear(*authority)
		return ErrLeaderAuthorityLost
	}
	if err != nil {
		return fmt.Errorf("validate leader authority: %w", err)
	}
	s.leadership.refresh(*authority, acquiredAt, heartbeatAt)
	return nil
}

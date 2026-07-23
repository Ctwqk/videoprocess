package channelops

import (
	"context"
	"errors"
	"strings"
	"time"
)

var ErrLiveSmokeLeaderActive = errors.New("channelops managed leader is active")

type SmokeResult struct {
	TaskScheduled       bool
	PublicationUnlisted bool
	MetricsWritten      bool
	LedgerRows          int
	TakedownRows        int
}

func (r SmokeResult) Validate() error {
	if !r.TaskScheduled {
		return errors.New("no task reached scheduled or measured")
	}
	if !r.PublicationUnlisted {
		return errors.New("publication was not confirmed unlisted")
	}
	if !r.MetricsWritten {
		return errors.New("metrics snapshot was not written")
	}
	if r.LedgerRows <= 0 {
		return errors.New("material_usage_ledger did not grow")
	}
	if r.TakedownRows != 0 {
		return errors.New("takedown_events is non-zero")
	}
	return nil
}

type LiveSmoke struct {
	Store    *Store
	Handler  HandlerService
	HolderID string
}

func (s LiveSmoke) Run(
	ctx context.Context,
	channelID string,
) (result SmokeResult, runErr error) {
	if s.Store == nil {
		return SmokeResult{}, errors.New("channelops live smoke store is not configured")
	}
	holderID := strings.TrimSpace(s.HolderID)
	if holderID == "" {
		return SmokeResult{}, errors.New("channelops live smoke holder id is required")
	}
	lease, acquired, err := s.Store.TryAcquireLeader(ctx, holderID, s.Store.Now())
	if err != nil {
		return SmokeResult{}, err
	}
	if !acquired || lease == nil {
		return SmokeResult{}, ErrLiveSmokeLeaderActive
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		releaseErr := lease.Release(releaseCtx, s.Store.Now())
		if releaseErr != nil {
			runErr = errors.Join(runErr, releaseErr)
		}
	}()

	authority := lease.Authority()
	if authority.HolderID != holderID || authority.Epoch <= 0 {
		return SmokeResult{}, ErrLeaderAuthorityUnavailable
	}
	handler := s.Handler
	handler.Store = s.Store
	return s.Store.RunLiveSmoke(ctx, channelID, handler)
}

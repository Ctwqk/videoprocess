package channelops

import (
	"context"
	"errors"
)

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
	Store   *Store
	Handler HandlerService
}

func (s LiveSmoke) Run(ctx context.Context, channelID string) (SmokeResult, error) {
	if s.Store == nil {
		return SmokeResult{}, errors.New("channelops live smoke store is not configured")
	}
	return s.Store.RunLiveSmoke(ctx, channelID, s.Handler)
}

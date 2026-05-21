package channelops

import "testing"

func TestSmokeResultRequiresLedgerAndNoTakedown(t *testing.T) {
	result := SmokeResult{
		TaskScheduled:       true,
		PublicationUnlisted: true,
		MetricsWritten:      true,
		LedgerRows:          1,
		TakedownRows:        0,
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	result.LedgerRows = 0
	if err := result.Validate(); err == nil {
		t.Fatal("expected missing ledger rows to fail validation")
	}

	result.LedgerRows = 1
	result.TakedownRows = 1
	if err := result.Validate(); err == nil {
		t.Fatal("expected takedown rows to fail validation")
	}
}

package harness

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMetricsTracker_BasicFlow(t *testing.T) {
	tracker := NewMetricsTracker()
	tracker.Start()

	// Émet 10 messages : 8 valides, 2 corrompus
	for i := 0; i < 8; i++ {
		tracker.RecordEmission(false)
		tracker.RecordSuccess()
	}
	tracker.RecordEmission(true)
	tracker.RecordEmission(true)

	// Simule 2 apoptoses et mitoses
	tracker.RecordApoptosis("cell-1")
	time.Sleep(2 * time.Millisecond)
	tracker.RecordMitosis("cell-1", 2)

	tracker.RecordApoptosis("cell-2")
	time.Sleep(3 * time.Millisecond)
	tracker.RecordMitosis("cell-2", 2)

	tracker.Stop()

	snap := tracker.Snapshot()
	if snap.TotalEmitted != 10 {
		t.Errorf("expected 10 emitted, got %d", snap.TotalEmitted)
	}
	if snap.TotalDelivered != 8 {
		t.Errorf("expected 8 delivered, got %d", snap.TotalDelivered)
	}
	if snap.TotalCorrupted != 2 {
		t.Errorf("expected 2 corrupted, got %d", snap.TotalCorrupted)
	}
	if snap.TotalApoptosis != 2 {
		t.Errorf("expected 2 apoptosis, got %d", snap.TotalApoptosis)
	}
	if snap.TotalMitosis != 2 {
		t.Errorf("expected 2 mitosis, got %d", snap.TotalMitosis)
	}

	mttr := tracker.MTTR()
	if mttr <= 0 {
		t.Errorf("expected positive MTTR, got %v", mttr)
	}

	report := tracker.Report()
	if !strings.Contains(report, "RAPPORT D'HOMÉOSTASIE") {
		t.Errorf("report missing title header")
	}
	if !strings.Contains(report, "cell-1") || !strings.Contains(report, "cell-2") {
		t.Errorf("report missing cell stats")
	}

	jsonReport := tracker.ReportJSON()
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(jsonReport), &raw); err != nil {
		t.Fatalf("invalid JSON report: %v", err)
	}
}

func TestMetricsTracker_NoApoptosis(t *testing.T) {
	tracker := NewMetricsTracker()
	tracker.RecordEmission(false)
	tracker.RecordSuccess()

	if tracker.MTTR() != 0 {
		t.Errorf("expected 0 MTTR when no apoptosis, got %v", tracker.MTTR())
	}
	if tracker.SurvivalRate() != 100.0 {
		t.Errorf("expected 100.0%% survival rate, got %f", tracker.SurvivalRate())
	}
}

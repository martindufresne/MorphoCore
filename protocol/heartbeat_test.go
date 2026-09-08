package protocol

import (
	"morphocore/dna"
	"testing"
	"time"
)

func TestSignalTypeString(t *testing.T) {
	cases := []struct {
		sig      SignalType
		expected string
	}{
		{SignalPing, "SignalPing"},
		{SignalMitosisRequest, "SignalMitosisRequest"},
		{SignalMitosisAck, "SignalMitosisAck"},
		{SignalRouteUpdate, "SignalRouteUpdate"},
		{SignalType(99), "UnknownSignal"},
	}

	for _, tc := range cases {
		if tc.sig.String() != tc.expected {
			t.Errorf("expected %s, got %s", tc.expected, tc.sig.String())
		}
	}
}

func TestPeerSignalPayload(t *testing.T) {
	sig := PeerSignal{
		Type:       SignalMitosisRequest,
		SenderID:   "cell-test",
		Generation: 2,
		Mitosis: MitosisRequestData{
			DeadCellID: "cell-test",
			Family:     dna.FamilyAxone,
			Generation: 2,
			DNAHash:    0x12345678,
		},
		Timestamp: time.Now(),
	}

	if sig.Mitosis.Family != dna.FamilyAxone {
		t.Errorf("expected FamilyAxone, got %v", sig.Mitosis.Family)
	}
}

package ibcv2

import "testing"

func TestTheEstimateIsKeptWithoutAMultiplier(t *testing.T) {
	if got := AdjustGasLimit(3_240_099, nil); got != 3_240_099 {
		t.Fatalf("expected the estimate, got %d", got)
	}
}

func TestAMultiplierAtOrBelowOneKeepsTheEstimate(t *testing.T) {
	for _, multiplier := range []float64{1, 0.5, 0} {
		m := multiplier
		if got := AdjustGasLimit(3_240_099, &m); got != 3_240_099 {
			t.Fatalf("multiplier %v: expected the estimate, got %d", m, got)
		}
	}
}

func TestAMultiplierRaisesTheLimitRoundingUp(t *testing.T) {
	m := 2.5
	if got := AdjustGasLimit(3_240_099, &m); got != 8_100_248 {
		t.Fatalf("expected 8100248, got %d", got)
	}
}

package reflowthroughput

import (
	"fmt"
	"math"
	"strings"
)

// HonestyResult is the universal ceiling/reason check for one point.
type HonestyResult struct {
	OK      bool
	Message string
}

// CheckHonesty enforces:
//
//	0 <= max_active <= sourceCap+destCap+probeCap
//	effective <= requested
//
// Source and probe domains cap at effective. Dest is bounded by the *recorded*
// dest_domain_ceiling_effective from the product run/summary (not a hard-coded
// multiplier). Multiplier is cross-checked when present so a non-default
// ConcurrencyConfig.DestDomainMultiplier cannot silently over-admit relative
// to the gate. Equality effective==requested requires reason exactly
// "requested". Lower effective requires resource_capped:* reason AND exactly
// one clamp warning. Honest caps are valid results, not harness failures.
func CheckHonesty(p ParsedReflowOutput, requested int) HonestyResult {
	if requested < 1 {
		return HonestyResult{OK: false, Message: "requested concurrency must be >= 1"}
	}
	maxA := p.MaxActive
	eff := p.Effective
	req := p.Requested
	if req == 0 {
		req = requested
	}
	if maxA < 0 || eff < 0 || req < 0 {
		return HonestyResult{OK: false, Message: "negative concurrency field"}
	}
	// Gate asserts resolved dest policy from product output — never assume ×2.
	mult := p.DestDomainMultiplier
	destCeil := p.DestDomainCeilingEffective
	if mult < 1 || destCeil < 1 {
		return HonestyResult{OK: false, Message: fmt.Sprintf(
			"missing dest-domain policy (multiplier=%d ceiling=%d); gate requires resolved run/summary fields",
			mult, destCeil)}
	}
	// Hard dest ceiling must not exceed mult×effective (product clamp invariant).
	if want := mult * eff; destCeil > want {
		return HonestyResult{OK: false, Message: fmt.Sprintf(
			"dest_domain_ceiling_effective %d > multiplier×effective %d (mult=%d effective=%d)",
			destCeil, want, mult, eff)}
	}
	// Optional stage-stats record must agree with run/summary policy when present.
	if st := p.ObjectPathStageStats; st != nil {
		if st.DestDomainMultiplier != 0 && st.DestDomainMultiplier != mult {
			return HonestyResult{OK: false, Message: fmt.Sprintf(
				"stage_stats dest_domain_multiplier %d != run/summary %d", st.DestDomainMultiplier, mult)}
		}
		if st.DestDomainCeilingEffective != 0 && st.DestDomainCeilingEffective != destCeil {
			return HonestyResult{OK: false, Message: fmt.Sprintf(
				"stage_stats dest_domain_ceiling_effective %d != run/summary %d",
				st.DestDomainCeilingEffective, destCeil)}
		}
	}
	// source + dest(recorded ceiling) + probe
	maxActiveCap := eff + destCeil + eff
	if maxA > maxActiveCap {
		return HonestyResult{OK: false, Message: fmt.Sprintf(
			"max_active %d > domain sum cap %d (effective %d, dest_ceiling %d, mult %d)",
			maxA, maxActiveCap, eff, destCeil, mult)}
	}
	if eff > req {
		return HonestyResult{OK: false, Message: fmt.Sprintf("effective %d > requested %d", eff, req)}
	}
	if eff == req {
		if p.Reason != "requested" {
			return HonestyResult{OK: false, Message: fmt.Sprintf("effective==requested requires reason %q, got %q", "requested", p.Reason)}
		}
		if p.WarningClampCount != 0 {
			return HonestyResult{OK: false, Message: fmt.Sprintf("effective==requested but clamp warnings=%d", p.WarningClampCount)}
		}
		return HonestyResult{OK: true, Message: "effective equals requested"}
	}
	// eff < req → resource cap
	if !strings.HasPrefix(p.Reason, "resource_capped:") {
		return HonestyResult{OK: false, Message: fmt.Sprintf("effective %d < requested %d but reason %q is not resource_capped:*", eff, req, p.Reason)}
	}
	if p.WarningClampCount != 1 {
		return HonestyResult{OK: false, Message: fmt.Sprintf("clamp requires exactly one REFLOW_CONCURRENCY_CEILING_CLAMPED warning, got %d", p.WarningClampCount)}
	}
	if !p.ClampWarningOK {
		return HonestyResult{OK: false, Message: "clamp warning flag not set"}
	}
	return HonestyResult{OK: true, Message: fmt.Sprintf("honest clamp effective=%d reason=%s", eff, p.Reason)}
}

// OccupancyResult is the fixed-mode reflow-saturation occupancy check.
type OccupancyResult struct {
	OK           bool
	Samples      []int
	Floor        int
	PassedNeeded int
	Passed       int
	Message      string
}

// CheckOccupancy applies the serial-dispatch detector for fixed-mode reflow saturation.
func CheckOccupancy(samples []int, effective int, passCount int) OccupancyResult {
	if effective < 1 {
		return OccupancyResult{OK: false, Message: "effective ceiling < 1"}
	}
	if passCount < 1 {
		passCount = 2
	}
	floor := int(math.Ceil(0.75 * float64(effective)))
	if floor < 1 {
		floor = 1
	}
	passed := 0
	for _, s := range samples {
		if s >= floor {
			passed++
		}
	}
	ok := passed >= passCount
	msg := fmt.Sprintf("occupancy floor=%d effective=%d passed=%d/%d need=%d samples=%v", floor, effective, passed, len(samples), passCount, samples)
	return OccupancyResult{
		OK:           ok,
		Samples:      append([]int(nil), samples...),
		Floor:        floor,
		PassedNeeded: passCount,
		Passed:       passed,
		Message:      msg,
	}
}

// CheckCounts enforces corpus/tap/terminal agreement for hard structural gates.
func CheckCounts(generated int, tapRows int64, completed int64, summaryErrors int64, invalid int64) error {
	if generated < 1 {
		return fmt.Errorf("generated count < 1")
	}
	if tapRows > 0 && int64(generated) != tapRows {
		return fmt.Errorf("tap rows %d != generated %d", tapRows, generated)
	}
	if completed != int64(generated) {
		return fmt.Errorf("completed objects %d != generated %d", completed, generated)
	}
	if summaryErrors != 0 {
		return fmt.Errorf("summary errors = %d", summaryErrors)
	}
	if invalid != 0 {
		return fmt.Errorf("invalid inputs = %d", invalid)
	}
	return nil
}

package reflowthroughput

import (
	"testing"
)

func TestCheckpointScaleScheduleCounterbalancesClassOrder(t *testing.T) {
	t.Parallel()
	spec, err := ResolveProfile(ProfileCheckpointScale)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{ScheduleDiskFirstOdd, ScheduleTmpfsFirstOdd} {
		t.Run(id, func(t *testing.T) {
			steps, err := CheckpointScaleSchedule(spec, id)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateCheckpointScaleSchedule(steps); err != nil {
				t.Fatal(err)
			}
			// Exactly 2 warmups + 6 retained arms.
			warm, retained := 0, 0
			for _, s := range steps {
				if s.Warmup {
					warm++
				} else {
					retained++
				}
			}
			if warm != 2 || retained != 6 {
				t.Fatalf("warm=%d retained=%d", warm, retained)
			}
			// Pair orders: disk_first_odd → D/T, T/D, D/T
			wantFirst := map[string][]string{
				ScheduleDiskFirstOdd:  {"disk", "tmpfs", "disk"},
				ScheduleTmpfsFirstOdd: {"tmpfs", "disk", "tmpfs"},
			}[id]
			for pair := 1; pair <= 3; pair++ {
				var first string
				for _, s := range steps {
					if !s.Warmup && s.PairIndex == pair {
						first = s.CheckpointClass
						break
					}
				}
				if first != wantFirst[pair-1] {
					t.Fatalf("pair %d first=%s want %s", pair, first, wantFirst[pair-1])
				}
			}
			// Within each pair: identical parallel (non-class provenance).
			for pair := 1; pair <= 3; pair++ {
				var pars []int
				var envs []string
				for _, s := range steps {
					if !s.Warmup && s.PairIndex == pair {
						pars = append(pars, s.Parallel)
						envs = append(envs, s.MemoryEnvelope)
					}
				}
				if len(pars) != 2 || pars[0] != pars[1] {
					t.Fatalf("pair %d parallel %v", pair, pars)
				}
				if envs[0] != envs[1] {
					t.Fatalf("pair %d envelope %v", pair, envs)
				}
			}
		})
	}
}

func TestCheckpointScaleScheduleRejectsBadID(t *testing.T) {
	t.Parallel()
	spec, err := ResolveProfile(ProfileCheckpointScale)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CheckpointScaleSchedule(spec, "not-a-schedule"); err == nil {
		t.Fatal("expected error")
	}
}

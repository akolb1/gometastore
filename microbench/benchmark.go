// Copyright © 2018 Alex Kolbasov
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package microbench

import (
	"time"
)

// repeat calls given function specified number of times
func repeat(f func(), count int) {
	for i := 0; i < count; i++ {
		f()
	}
}

// measure calls function f and measures time it takes to run it
func measure(f func(), s *Stats) {
	start := time.Now()
	f()
	s.Add(float64(time.Since(start).Nanoseconds()))
}

// MeasureSimple calls given function f multiple times and returns times for each run.
// It has a warmup phase during which times are not collected and execution phase
// during which actual times are collected. Params:
//
//   warmup - number of warmup iterations
//   iterations - number of measured iterations
func MeasureSimple(f func(), warmup int, iterations int) *Stats {
	repeat(f, warmup)
	stats := MakeStats()
	repeat(func() {
		measure(f, stats)
	}, iterations)
	return stats
}

// Measure calls given function f multiple times and returns times for each run.
// Each iteration has 3 phases:
//   setup - prepares the test
//   test - actual test
//   cleanup - cleans up after the test
//
// Measurement has a warmup phase during which times are not collected and execution phase
// during which actual times are collected. Params:
//
//   pre - setup function, may be nil
//   what - actual test function, must be non-nil
//   post - cleanup function, may be nil
//   warmup - number of warmup iterations
//   iterations - number of measured iterations
func Measure(pre func(), what func(), post func(), warmupCount int, iterations int) *Stats {
	stats := MakeStats()
	warmup := func() {
		if pre != nil {
			pre()
		}
		what()
		if post != nil {
			post()
		}
	}

	experiment := func() {
		if pre != nil {
			pre()
		}
		measure(what, stats)
		if post != nil {
			post()
		}
	}
	repeat(warmup, warmupCount)
	repeat(experiment, iterations)
	return stats
}

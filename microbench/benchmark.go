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

func repeat(f func(), count int) {
	for i := 0; i < count; i++ {
		f()
	}
}

func measure(f func(), s *Stats) {
	start := time.Now()
	f()
	s.Add(float64(time.Since(start).Nanoseconds()))
}

func MeasureSimple(f func(), warmup int, iterations int) *Stats {
	repeat(f, warmup)
	stats := MakeStats()
	repeat(func() {
		measure(f, stats)
	}, iterations)
	return stats
}

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

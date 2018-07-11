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
	"bytes"
	"fmt"
	"log"
	"sort"
)

// Runner is any function that produces Stats.
type Runner func() *Stats

// BenchmarkSuite is a collection of individual benchmarks.
// Each benchmark has a name, associated Runner and associated Stats once it is executed.
type BenchmarkSuite struct {
	scale      float64
	sanitize   bool
	names      []string
	benchmarks map[string]Runner
	results    map[string]*Stats
}

// MakeBenchmarkSuite returns a new instance of BenchmarkSuite.
// Parameters:
//  scale - time scale (1 means results are collected in nanoseconds, 1000 means milliseconds, etc).
//  sanitize - if true, outliers are removed when results are presented.
func MakeBenchmarkSuite(scale int, sanitize bool) *BenchmarkSuite {
	return &BenchmarkSuite{
		scale:      float64(scale),
		sanitize:   sanitize,
		names:      []string{},
		benchmarks: make(map[string]Runner),
		results:    make(map[string]*Stats),
	}
}

// Add benchmark to the suite.
// Parameters:
//   name - benchmark name
//   f - benchmark runner
func (b *BenchmarkSuite) Add(name string, f Runner) *BenchmarkSuite {
	b.names = append(b.names, name)
	b.benchmarks[name] = f
	return b
}

// List returns list of benchmarks names
func (b *BenchmarkSuite) List() []string {
	result := b.names
	sort.Strings(result)
	return result
}

// Run all benchmarks in the suite
func (b *BenchmarkSuite) Run() *BenchmarkSuite {
	for _, name := range b.names {
		log.Println("Running", name)
		result := b.benchmarks[name]()
		if result == nil {
			continue
		}
		if b.sanitize {
			result = result.Sanitized()
		}
		b.results[name] = result
	}
	return b
}

// RunSelected runs only selected banchmarks
func (b *BenchmarkSuite) RunSelected(names []string) *BenchmarkSuite {
	for _, name := range names {
		bench, ok := b.benchmarks[name]
		if !ok {
			log.Println("Skipping", name, ": not found")
			continue
		}
		log.Println("Running", name)
		result := bench()
		if result == nil {
			continue
		}
		if b.sanitize {
			result = result.Sanitized()
		}
		b.results[name] = result
	}
	return b
}

// Display benchmark results as a formatted table.
// Parameters:
//  buffer - output buffer
func (b *BenchmarkSuite) Display(buffer *bytes.Buffer) {
	buffer.WriteString(fmt.Sprintf("%-30s %-8s %-8s %-8s %-8s\n",
		"Operation", "Mean", "Min", "Max", "Err%"))
	for _, name := range b.names {
		result := b.results[name]
		if result == nil {
			continue
		}
		mean := result.Mean()
		err := result.StDev() * 100 / mean
		buffer.WriteString(fmt.Sprintf("%-30s %-8.4g %-8.4g %-8.4g %-8.4g\n",
			name, mean/b.scale, result.Min()/b.scale, result.Max()/b.scale, err))
	}
}

// DisplayCSV displays results in CSV format
// Parameters:
//   buffer - output buffer
//   separator - column separator
func (b *BenchmarkSuite) DisplayCSV(buffer *bytes.Buffer, separator string) {
	buffer.WriteString(fmt.Sprintf("Operation%sMean%sMin%sMax%sErr%%\n",
		separator, separator, separator, separator))
	for _, name := range b.names {
		result := b.results[name]
		if result == nil {
			continue
		}
		mean := result.Mean()
		err := result.StDev() * 100 / mean
		buffer.WriteString(fmt.Sprintf("%s%s%g%s%g%s%g%s%g\n",
			name, separator,
			mean/b.scale, separator,
			result.Min()/b.scale, separator,
			result.Max()/b.scale, separator,
			err))
	}
}

// GetResults returns raw test results
func (b *BenchmarkSuite) GetResults() map[string]*Stats {
	return b.results
}

// Copyright © 2017 Alex Kolbasov
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
)

type runner func() *Stats

type BenchmarkSuite struct {
	scale      float64
	sanitize   bool
	names      []string
	benchmarks map[string]runner
	results    map[string]*Stats
}

func MakeBenchmarkSuite(scale int, sanitize bool) *BenchmarkSuite {
	return &BenchmarkSuite{
		scale:      float64(scale),
		sanitize:   sanitize,
		names:      []string{},
		benchmarks: make(map[string]runner),
		results:    make(map[string]*Stats),
	}
}

func (b *BenchmarkSuite) Add(name string, f runner) *BenchmarkSuite {
	b.names = append(b.names, name)
	b.benchmarks[name] = f
	return b
}

func (b *BenchmarkSuite) List() []string {
	return b.names
}

func (b *BenchmarkSuite) Run() *BenchmarkSuite {
	for _, name := range b.names {
		log.Println("Running", name)
		result := b.benchmarks[name]()
		if b.sanitize {
			result = result.Sanitized()
		}
		b.results[name] = result
	}
	return b
}

func (b *BenchmarkSuite) Display(buffer *bytes.Buffer) {
	buffer.WriteString(fmt.Sprintf("%-30s %-8s %-8s %-8s %-8s\n",
		"Operation", "Mean", "Min", "Max", "Err%"))
	for _, name := range b.names {
		result := b.results[name]
		mean := result.Mean()
		err := result.StDev() * 100 / mean
		buffer.WriteString(fmt.Sprintf("%-30s %-8.3g %-8.3g %-8.3g %-8.3g\n",
			name, mean/b.scale, result.Min()/b.scale, result.Max()/b.scale, err))
	}
}

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
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
)

type Stats struct {
	data []float64
}

func MakeStats() *Stats {
	return &Stats{data: []float64{}}
}

func (dt *Stats) Add(val float64) {
	dt.data = append(dt.data, val)
}

// Mean computes the mean value of data
func (dt *Stats) Mean() float64 {
	return stat.Mean(dt.data, nil)
}

func (dt *Stats) Min() float64 {
	return floats.Min(dt.data)
}

func (dt *Stats) Max() float64 {
	return floats.Max(dt.data)
}

func (dt *Stats) StDev() float64 {
	return stat.StdDev(dt.data, nil)
}

// Sanitized returns sanitized statistics which has any datapoints outside
// of mean +/- stdev removed.
func (dt *Stats) Sanitized() *Stats {
	mean := dt.Mean()
	delta := 2 * dt.StDev()
	minVal := mean - delta
	maxVal := mean + delta
	result := []float64{}
	for _, v := range dt.data {
		if v >= minVal && v <= maxVal {
			result = append(result, v)
		}
	}
	return &Stats{result}
}

// Write writes data as string representation, one per line
func (dt *Stats) Write(buffer bytes.Buffer) {
	for _, v := range dt.data {
		buffer.WriteString(fmt.Sprintf("%g\n", v))
	}
}

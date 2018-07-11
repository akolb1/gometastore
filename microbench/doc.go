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

/*
Package microbench provides support for simple microbenchmark. It supports benchmarks
that have some set-up and cleanup for each iteration.

Example usage;

func benchCreateTable(data *benchData) *microbench.Stats {
	table := hmsclient.NewTableBuilder(data.dbname, testTableName).
		WithOwner(data.owner).
		WithColumns(getSchema(testSchema)).
		Build()
	return microbench.Measure(nil,
		func() { data.client.CreateTable(table) },
		func() { data.client.DropTable(data.dbname, testTableName, true) },
		data.warmup, data.iterations)
}

Example usage:

  import(
	"github.com/akolb1/gometastore/microbench"
  )

func benchSomething(warmup int, iterations int) *microbench.Stats {
	return microbench.Measure(
		func() { do_some_setup() },
		func() { do_some_work_to_measure },
		func() { do_some_cleanup() },
		warmup, iterations)
}
*/
package microbench

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

package cmd

import (
	"github.com/akolb1/gometastore/hmsclient"
	"github.com/akolb1/gometastore/microbench"
)

type benchData struct {
	warmup     int
	iterations int
	dbname     string
	client     *hmsclient.MetastoreClient
}

func makeBenchData(warmup int, iterations int, dbName string,
	client *hmsclient.MetastoreClient) *benchData {
	return &benchData{
		warmup:     warmup,
		iterations: iterations,
		client:     client,
		dbname:     dbName,
	}
}

func benchListDatabases(data *benchData) *microbench.Stats {
	return microbench.MeasureSimple(func() {
		data.client.GetAllDatabases()
	}, data.warmup, data.iterations)
}

func benchGetDatabase(data *benchData) *microbench.Stats {
	return microbench.MeasureSimple(func() {
		data.client.GetAllDatabases()
	}, data.warmup, data.iterations)
}

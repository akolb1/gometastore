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
	"log"

	"github.com/akolb1/gometastore/hmsclient"
	"github.com/akolb1/gometastore/microbench"
)

const (
	testTableName = "test_table"
	testSchema    = "name"
)

type benchData struct {
	warmup     int
	iterations int
	dbname     string
	owner      string
	client     *hmsclient.MetastoreClient
}

func makeBenchData(warmup int, iterations int, dbName string, owner string,
	client *hmsclient.MetastoreClient) *benchData {
	return &benchData{
		warmup:     warmup,
		iterations: iterations,
		client:     client,
		dbname:     dbName,
		owner:      owner,
	}
}

func benchListDatabases(data *benchData) *microbench.Stats {
	return microbench.MeasureSimple(func() {
		data.client.GetAllDatabases()
	}, data.warmup, data.iterations)
}

func benchGetDatabase(data *benchData) *microbench.Stats {
	if err := data.client.CreateDatabase(&hmsclient.Database{Name: data.dbname}); err != nil {
		log.Fatalf("failed to drop database %s: %v", data.dbname, err)
	}
	defer data.client.DropDatabase(data.dbname, true, true)

	return microbench.MeasureSimple(func() {
		data.client.GetAllDatabases()
	}, data.warmup, data.iterations)
}

func benchCreateDatabase(data *benchData) *microbench.Stats {
	return microbench.Measure(nil,
		func() { data.client.CreateDatabase(&hmsclient.Database{Name: data.dbname}) },
		func() { data.client.DropDatabase(data.dbname, true, true) },
		data.warmup, data.iterations)
}

func benchDropDatabase(data *benchData) *microbench.Stats {
	return microbench.Measure(
		func() { data.client.CreateDatabase(&hmsclient.Database{Name: data.dbname}) },
		func() { data.client.DropDatabase(data.dbname, true, true) },
		nil,
		data.warmup, data.iterations)
}

func benchCreateTable(data *benchData) *microbench.Stats {
	if err := data.client.CreateDatabase(&hmsclient.Database{Name: data.dbname}); err != nil {
		log.Fatalf("failed to drop database %s: %v", data.dbname, err)
	}
	defer data.client.DropDatabase(data.dbname, true, true)
	table := hmsclient.MakeTable(data.dbname,
		testTableName, data.owner, nil,
		getSchema(testSchema), nil)

	return microbench.Measure(nil,
		func() { data.client.CreateTable(table) },
		func() { data.client.DropTable(data.dbname, testTableName, true) },
		data.warmup, data.iterations)
}

func benchDropTable(data *benchData) *microbench.Stats {
	if err := data.client.CreateDatabase(&hmsclient.Database{Name: data.dbname}); err != nil {
		log.Fatalf("failed to drop database %s: %v", data.dbname, err)
	}
	defer data.client.DropDatabase(data.dbname, true, true)

	table := hmsclient.MakeTable(data.dbname,
		testTableName, data.owner, nil,
		getSchema(testSchema), nil)

	return microbench.Measure(
		func() { data.client.CreateTable(table) },
		func() { data.client.DropTable(data.dbname, testTableName, true) },
		nil,
		data.warmup, data.iterations)
}

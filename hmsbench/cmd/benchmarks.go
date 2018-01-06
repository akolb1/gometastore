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

	"fmt"

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
	nObjects   int
	dbname     string
	owner      string
	client     *hmsclient.MetastoreClient
}

func makeBenchData(warmup int, iterations int, dbName string, owner string,
	client *hmsclient.MetastoreClient, nObjects int) *benchData {
	return &benchData{
		warmup:     warmup,
		iterations: iterations,
		client:     client,
		dbname:     dbName,
		owner:      owner,
		nObjects:   nObjects,
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

func benchGetTable(data *benchData) *microbench.Stats {
	dbName := data.dbname
	if err := data.client.CreateDatabase(&hmsclient.Database{Name: dbName}); err != nil {
		log.Fatalf("failed to drop database %s: %v", data.dbname, err)
	}
	defer data.client.DropDatabase(dbName, true, true)
	table := hmsclient.MakeTable(data.dbname,
		testTableName, data.owner, nil,
		getSchema(testSchema), nil)
	if err := data.client.CreateTable(table); err != nil {
		log.Println("failed to create table: ", err)
		return nil
	}

	defer data.client.DropTable(dbName, testTableName, true)

	return microbench.MeasureSimple(func() { data.client.GetTable(dbName, testTableName) },
		data.warmup, data.iterations)
}

// benchListManyTables creates a database with many tables and measures time to list all tables
func benchListManyTables(data *benchData) *microbench.Stats {
	dbName := data.dbname
	if err := data.client.CreateDatabase(&hmsclient.Database{Name: dbName}); err != nil {
		log.Fatalf("failed to drop database %s: %v", data.dbname, err)
	}
	defer data.client.DropDatabase(dbName, true, true)
	tableNames := make([]string, data.nObjects)
	// Create a bunch of tables
	for i := 0; i < data.nObjects; i++ {
		tableNames[i] = fmt.Sprintf("table_%d", i)
		table := hmsclient.MakeTable(dbName,
			tableNames[i], data.owner, nil,
			getSchema(testSchema), nil)
		if err := data.client.CreateTable(table); err != nil {
			log.Println("failed to create table: ", err)
			// Cleanup
			for j := 0; j < i; j++ {
				data.client.DropTable(dbName, tableNames[j], true)
			}
			return nil
		}
	}
	stats := microbench.MeasureSimple(func() { data.client.GetAllTables(dbName) },
		data.warmup, data.iterations)
	// cleanup
	for i := 0; i < data.nObjects; i++ {
		data.client.DropTable(data.dbname, tableNames[i], true)
	}
	return stats
}

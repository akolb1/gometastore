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
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/akolb1/gometastore/microbench"
)

const (
	testTableName       = "test_table"
	testSchema          = "name"
	testPartitionSchema = "date"
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
	return withDatabase(data,
		func() *microbench.Stats {
			return microbench.MeasureSimple(func() {
				data.client.GetAllDatabases()
			}, data.warmup, data.iterations)
		})
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
	table := hmsclient.MakeTable(data.dbname,
		testTableName, data.owner, nil,
		getSchema(testSchema), nil)
	return withDatabase(data,
		func() *microbench.Stats {
			return microbench.Measure(nil,
				func() { data.client.CreateTable(table) },
				func() { data.client.DropTable(data.dbname, testTableName, true) },
				data.warmup, data.iterations)
		})
}

func benchDropTable(data *benchData) *microbench.Stats {
	table := hmsclient.MakeTable(data.dbname,
		testTableName, data.owner, nil,
		getSchema(testSchema), nil)

	return withDatabase(data,
		func() *microbench.Stats {
			return microbench.Measure(
				func() { data.client.CreateTable(table) },
				func() { data.client.DropTable(data.dbname, testTableName, true) },
				nil,
				data.warmup, data.iterations)
		})
}

func benchGetTable(data *benchData) *microbench.Stats {
	dbName := data.dbname
	table := hmsclient.MakeTable(data.dbname,
		testTableName, data.owner, nil,
		getSchema(testSchema), nil)

	return withDatabase(data,
		func() *microbench.Stats {
			if err := data.client.CreateTable(table); err != nil {
				log.Println("failed to create table: ", err)
				return nil
			}

			defer data.client.DropTable(dbName, testTableName, true)

			return microbench.MeasureSimple(func() { data.client.GetTable(dbName, testTableName) },
				data.warmup, data.iterations)
		})
}

// benchListManyTables creates a database with many tables and measures time to list all tables
func benchListManyTables(data *benchData) *microbench.Stats {
	dbName := data.dbname
	tableNames := make([]string, data.nObjects)

	return withDatabase(data,
		func() *microbench.Stats {
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
		})
}

func benchAddPartition(data *benchData) *microbench.Stats {
	dbName := data.dbname

	return withDatabase(data,
		func() *microbench.Stats {
			if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
				log.Println("failed to create partition: ", err)
				return nil
			}
			defer data.client.DropTable(dbName, testTableName, true)
			table, err := data.client.GetTable(dbName, testTableName)
			if err != nil {
				log.Println("failed to get table: ", err)
			}
			values := []string{"d1"}
			partition, _ := hmsclient.MakePartition(table, values, nil, "")
			return microbench.Measure(nil,
				func() { data.client.AddPartition(partition) },
				func() { data.client.DropPartition(dbName, testTableName, values, true) },
				data.warmup, data.iterations)
		})
}

func benchDropPartition(data *benchData) *microbench.Stats {
	dbName := data.dbname

	return withDatabase(data,
		func() *microbench.Stats {
			if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
				log.Println("failed to create table: ", err)
				return nil
			}
			defer data.client.DropTable(dbName, testTableName, true)
			table, err := data.client.GetTable(dbName, testTableName)
			if err != nil {
				log.Println("failed to get table: ", err)
			}
			values := []string{"d1"}
			partition, _ := hmsclient.MakePartition(table, values, nil, "")
			return microbench.Measure(func() { data.client.AddPartition(partition) },
				func() { data.client.DropPartition(dbName, testTableName, values, true) },
				nil,
				data.warmup, data.iterations)
		})
}

func benchCreatePartitions(data *benchData) *microbench.Stats {
	dbName := data.dbname

	return withDatabase(data,
		func() *microbench.Stats {
			if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
				log.Println("failed to create table: ", err)
				return nil
			}
			table, err := data.client.GetTable(dbName, testTableName)
			if err != nil {
				log.Println("failed to get table: ", err)
			}
			partitions := makeManyPartitions(table, data.nObjects)
			names := makePartNames(data.nObjects)
			return microbench.Measure(nil,
				func() { addPartitions(data.client, partitions) },
				func() { dropManyPartitions(data, names) },
				data.warmup, data.iterations)
		})
}

func benchDropPartitions(data *benchData) *microbench.Stats {
	dbName := data.dbname

	return withDatabase(data,
		func() *microbench.Stats {
			if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
				log.Println("failed to create table: ", err)
				return nil
			}
			table, err := data.client.GetTable(dbName, testTableName)
			if err != nil {
				log.Println("failed to get table: ", err)
			}
			partitions := makeManyPartitions(table, data.nObjects)
			names := makePartNames(data.nObjects)
			return microbench.Measure(
				func() { addPartitions(data.client, partitions) },
				func() { dropManyPartitions(data, names) },
				nil,
				data.warmup, data.iterations)
		})
}

// createPartitionedTable creates a simple partitioned table with a single partition
func createPartitionedTable(client *hmsclient.MetastoreClient, dbName string,
	tableName string, owner string) error {
	table := hmsclient.MakeTable(dbName,
		tableName, owner, nil,
		getSchema(testSchema), getSchema(testPartitionSchema))
	return client.CreateTable(table)
}

// withDatabase creates database, runs the benchmark and then removes the database
func withDatabase(data *benchData, bench microbench.Runner) *microbench.Stats {
	dbName := data.dbname
	if err := data.client.CreateDatabase(&hmsclient.Database{Name: dbName}); err != nil {
		log.Fatalf("failed to drop database %s: %v", data.dbname, err)
	}
	defer data.client.DropDatabase(dbName, true, true)
	return bench()
}

// makeManyPartitions creates list of Partitions suitable for bulk creation
func makeManyPartitions(table *hive_metastore.Table, count int) []*hive_metastore.Partition {
	result := make([]*hive_metastore.Partition, count)
	for i := 0; i < count; i++ {
		values := []string{fmt.Sprintf("d%d", i)}
		partition, _ := hmsclient.MakePartition(table, values, nil, "")
		result[i] = partition
	}
	return result
}

// makePartNames creates a list of sample partition names of the form 'date=dX' for 0 <= X < count
func makePartNames(count int) []string {
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("%s=d%d", testPartitionSchema, i)
	}
	return names
}

// dropManyPartitions drops multiple partition by names and logs an error if this fails.
func dropManyPartitions(data *benchData, names []string) {
	err := data.client.DropPartitions(data.dbname, testTableName, names)
	if err != nil {
		log.Println("failed to drop partitions", err)
	}
}

func addPartitions(client *hmsclient.MetastoreClient, parts []*hive_metastore.Partition) {
	err := client.AddPartitions(parts)
	if err != nil {
		log.Println("failed to create partition", err)
	}
}

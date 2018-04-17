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
	"fmt"
	"log"

	"github.com/akolb1/gometastore/hmsclient"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/akolb1/gometastore/microbench"
	"github.com/mohae/deepcopy"
)

const (
	testTableName       = "test_table"
	testSchema          = "name"
	testPartitionSchema = "date"
	defaultTableType    = hmsclient.TableTypeManaged
)

type benchData struct {
	warmup     int
	iterations int
	nObjects   int
	nThreads   int
	dbname     string
	owner      string
	client     *hmsclient.MetastoreClient
}

// Channel type used to report thread completion
type completion struct{}

func makeBenchData(warmup int, iterations int, dbName string, owner string,
	client *hmsclient.MetastoreClient, nObjects int, nThreads int) *benchData {
	return &benchData{
		warmup:     warmup,
		iterations: iterations,
		client:     client,
		dbname:     dbName,
		owner:      owner,
		nObjects:   nObjects,
		nThreads:   nThreads,
	}
}

// GetCurrentNotificationId() benchmark
func benchGetNotificationId(data *benchData) *microbench.Stats {
	return microbench.MeasureSimple(func() {
		data.client.GetCurrentNotificationId()
	}, data.warmup, data.iterations)
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

func benchCreateDatabase(data *benchData) *microbench.Stats {
	tmpDb := data.dbname + "_tmp"
	return microbench.Measure(nil,
		func() { data.client.CreateDatabase(&hmsclient.Database{Name: tmpDb}) },
		func() { data.client.DropDatabase(tmpDb, true, true) },
		data.warmup, data.iterations)
}

func benchDropDatabase(data *benchData) *microbench.Stats {
	tmpDb := data.dbname + "_tmp"
	return microbench.Measure(
		func() { data.client.CreateDatabase(&hmsclient.Database{Name: tmpDb}) },
		func() { data.client.DropDatabase(tmpDb, true, true) },
		nil,
		data.warmup, data.iterations)
}

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

func benchDropTable(data *benchData) *microbench.Stats {
	table := hmsclient.NewTableBuilder(data.dbname, testTableName).
		WithOwner(data.owner).
		WithColumns(getSchema(testSchema)).
		Build()
	return microbench.Measure(
		func() { data.client.CreateTable(table) },
		func() { data.client.DropTable(data.dbname, testTableName, true) },
		nil,
		data.warmup, data.iterations)
}

func benchGetTable(data *benchData) *microbench.Stats {
	dbName := data.dbname
	table := hmsclient.NewTableBuilder(data.dbname, testTableName).
		WithOwner(data.owner).
		WithColumns(getSchema(testSchema)).
		Build()
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
	tableNames := make([]string, data.nObjects)

	// Create a bunch of tables
	for i := 0; i < data.nObjects; i++ {
		tableNames[i] = fmt.Sprintf("table_%d", i)
		table := hmsclient.NewTableBuilder(dbName, tableNames[i]).
			WithOwner(data.owner).
			WithColumns(getSchema(testSchema)).
			Build()
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

func benchAddPartition(data *benchData) *microbench.Stats {
	dbName := data.dbname
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
}

func benchDropPartition(data *benchData) *microbench.Stats {
	dbName := data.dbname
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
}

func benchCreatePartitions(data *benchData) *microbench.Stats {
	dbName := data.dbname
	if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
		log.Println("failed to create table: ", err)
		return nil
	}
	defer data.client.DropTable(dbName, testTableName, true)

	table, err := data.client.GetTable(dbName, testTableName)
	if err != nil {
		log.Println("failed to get table: ", err)
		return nil
	}

	prefix := "d"
	partitions := makeManyPartitions(table, prefix, data.nObjects)
	names := makePartNames(prefix, data.nObjects)
	return microbench.Measure(nil,
		func() { addPartitions(data.client, partitions) },
		func() { dropManyPartitions(data, names) },
		data.warmup, data.iterations)
}

// benchGetPartitions create a table with N partitions and measure time to get all N
// partitions
func benchGetPartitions(data *benchData) *microbench.Stats {
	dbName := data.dbname
	if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
		log.Println("failed to create table: ", err)
		return nil
	}
	defer data.client.DropTable(dbName, testTableName, true)
	table, err := data.client.GetTable(dbName, testTableName)
	if err != nil {
		log.Println("failed to get table: ", err)
		return nil
	}
	prefix := "d"
	partitions := makeManyPartitions(table, prefix, data.nObjects)
	names := makePartNames(prefix, data.nObjects)
	addPartitions(data.client, partitions)
	defer dropManyPartitions(data, names)
	return microbench.MeasureSimple(func() {
		data.client.GetPartitions(data.dbname, testTableName, -1)
	}, data.warmup, data.iterations)
}

func benchDropPartitions(data *benchData) *microbench.Stats {
	dbName := data.dbname
	if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
		log.Println("failed to create table: ", err)
		return nil
	}
	defer data.client.DropTable(dbName, testTableName, true)

	table, err := data.client.GetTable(dbName, testTableName)
	if err != nil {
		log.Println("failed to get table: ", err)
	}
	prefix := "d"
	partitions := makeManyPartitions(table, prefix, data.nObjects)
	names := makePartNames(prefix, data.nObjects)
	return microbench.Measure(
		func() { addPartitions(data.client, partitions) },
		func() { dropManyPartitions(data, names) },
		nil,
		data.warmup, data.iterations)
}

func benchTableRenameWithPartitions(data *benchData) *microbench.Stats {
	dbName := data.dbname
	newName := testTableName + "_renamed"
	if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
		log.Println("failed to create table: ", err)
		return nil
	}
	defer data.client.DropTable(dbName, testTableName, true)

	table, err := data.client.GetTable(dbName, testTableName)
	if err != nil {
		log.Println("failed to get table: ", err)
	}
	partitions := makeManyPartitions(table, "d", data.nObjects)
	addPartitions(data.client, partitions)
	table.Sd.Location = ""
	var newTable *hive_metastore.Table
	newTable, _ = deepcopy.Copy(table).(*hive_metastore.Table)
	newTable.TableName = newName

	return microbench.MeasureSimple(
		func() {
			data.client.AlterTable(dbName, testTableName, newTable)
			data.client.AlterTable(dbName, newName, table)
		}, data.warmup, data.iterations)
}

func benchAddPartitionsInParallel(data *benchData) *microbench.Stats {
	dbName := data.dbname
	if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
		log.Println("failed to create table: ", err)
		return nil
	}
	defer data.client.DropTable(dbName, testTableName, true)

	table, err := data.client.GetTable(dbName, testTableName)
	if err != nil {
		log.Println("failed to get table: ", err)
		return nil
	}
	return microbench.MeasureSimple(
		func() {
			done := make(chan completion)
			for i := 0; i < data.nThreads; i++ {
				go addDropPartitions(data.client, done, table, data.nObjects, i)
			}
			// Wait for async routines to complete
			for i := 0; i < data.nThreads; i++ {
				<-done
			}
		}, data.warmup, data.iterations)
}

func benchTableRename(data *benchData) *microbench.Stats {
	dbName := data.dbname
	newName := testTableName + "_renamed"
	if err := createPartitionedTable(data.client, dbName, testTableName, data.owner); err != nil {
		log.Println("failed to create table: ", err)
		return nil
	}
	defer data.client.DropTable(dbName, testTableName, true)

	table, err := data.client.GetTable(dbName, testTableName)
	if err != nil {
		log.Println("failed to get table: ", err)
	}
	table.Sd.Location = ""
	var newTable *hive_metastore.Table
	newTable, _ = deepcopy.Copy(table).(*hive_metastore.Table)
	newTable.TableName = newName

	return microbench.MeasureSimple(
		func() {
			data.client.AlterTable(dbName, testTableName, newTable)
			data.client.AlterTable(dbName, newName, table)
		}, data.warmup, data.iterations)
}

func benchDeleteTableWithPartitions(data *benchData) *microbench.Stats {
	dbName := data.dbname
	client := data.client
	return microbench.Measure(
		func() {
			createPartitionedTable(client, dbName, testTableName, data.owner)
			table, _ := client.GetTable(dbName, testTableName)
			addPartitions(client,
				makeManyPartitions(table, "d", data.nObjects))
		},
		func() {
			client.DropTable(dbName, testTableName, true)
		},
		nil,
		data.warmup, data.iterations)
}

// createPartitionedTable creates a simple partitioned table with a single partition
func createPartitionedTable(client *hmsclient.MetastoreClient, dbName string,
	tableName string, owner string) error {
	table := hmsclient.NewTableBuilder(dbName, tableName).
		WithOwner(owner).
		WithColumns(getSchema(testSchema)).
		WithPartitionKeys(getSchema(testPartitionSchema)).
		Build()
	return client.CreateTable(table)
}

// makeManyPartitions creates list of Partitions suitable for bulk creation
func makeManyPartitions(table *hive_metastore.Table, prefix string, count int) []*hive_metastore.Partition {
	result := make([]*hive_metastore.Partition, count)
	for i := 0; i < count; i++ {
		values := []string{fmt.Sprintf("%s%d", prefix, i)}
		partition, _ := hmsclient.MakePartition(table, values, nil, "")
		result[i] = partition
	}
	return result
}

// makePartNames creates a list of sample partition names of the form 'date=dX' for 0 <= X < count
func makePartNames(prefix string, count int) []string {
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("%s=%s%d", testPartitionSchema, prefix, i)
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

func addDropPartitions(c *hmsclient.MetastoreClient, done chan completion,
	table *hive_metastore.Table,
	instances int, instance int) {
	client, err := c.Clone()
	if err != nil {
		log.Println(err)
		return
	}
	defer client.Close()
	prefix := fmt.Sprintf("d%d", instance)
	addPartitions(client,
		makeManyPartitions(table, prefix, instances))
	names := makePartNames(prefix, instances)
	err = client.DropPartitions(table.DbName, table.TableName, names)
	if err != nil {
		log.Println("failed to drop partitions", err)
	}
	done <- completion{}
}

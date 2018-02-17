package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/akolb1/gometastore/hmsclient"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "import HMS data",
	Run:   importData,
}

func importData(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	dbs, err := getDatabases(client)
	if err != nil {
		log.Fatal(err)
	}

	for _, arg := range args {
		err = doImport(client, dbs, arg)
		if err != nil {
			log.Println("failed to import", arg, err)
		}
	}
}

func doImport(client *hmsclient.MetastoreClient, dbMap map[string]bool, fileName string) error {
	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("failed to read file%s: %s", fileName, err.Error())
	}
	var hms HmsObject
	err = json.Unmarshal(raw, &hms)
	if err != nil {
		return fmt.Errorf("failed to parse file%s: %s", fileName, err.Error())
	}

	err = importDb(client, dbMap, hms.Databases)
	if err != nil {
		return fmt.Errorf("failed to import databases from %s: %s", fileName, err.Error())
	}
	// Cached table names
	tableMap := make(map[string]bool)
	err = importTables(client, dbMap, tableMap, hms.Tables)
	if err != nil {
		return fmt.Errorf("failed to import tables from %s: %s", fileName, err.Error())
	}
	err = importPartitions(client, dbMap, tableMap, hms.Partitions)
	if err != nil {
		return fmt.Errorf("failed to import partitions from %s: %s", fileName, err.Error())
	}
	return nil
}

func importDb(client *hmsclient.MetastoreClient,
	dbMap map[string]bool,
	dbs []*hmsclient.Database) error {
	for _, db := range dbs {
		if !dbMap[db.Name] {
			// There may be issues with re-using location, so drop our location
			db.Location = ""
			if err := client.CreateDatabase(db); err != nil {
				return fmt.Errorf("failed to create database %s: %s",
					db.Name, err.Error())
			}
			dbMap[db.Name] = true
		}
	}

	return nil
}

func importTables(client *hmsclient.MetastoreClient,
	dbMap map[string]bool,
	tableMap map[string]bool,
	tables []*hive_metastore.Table) error {
	for _, table := range tables {
		dbName := table.DbName
		tableName := table.TableName
		if !dbMap[dbName] {
			log.Println("skipping", dbName, ".", tableName, "db is not available")
			continue
		}
		fullTblName := dbName + "." + tableName
		if tableMap[fullTblName] {
			log.Println("skipping", dbName, ".", tableName, ": table exist already")
			continue
		}
		tbl, err := client.GetTable(dbName, tableName)
		if tbl != nil {
			tableMap[fullTblName] = true
			log.Println("skipping", dbName, ".", tableName, ": table exist already")
			continue
		}
		table.TableType = hmsclient.TableTypeExternal.String()
		log.Println("Adding table", dbName, ".", tableName)
		err = client.CreateTable(table)
		if err != nil {
			return fmt.Errorf("failed to create table %s.%s: %s",
				dbName, tableName, err.Error())
		}
		tableMap[fullTblName] = true
	}
	return nil
}

func importPartitions(client *hmsclient.MetastoreClient,
	dbMap map[string]bool,
	tableMap map[string]bool,
	partitions []*hive_metastore.Partition) error {
	for _, partition := range partitions {
		dbName := partition.DbName
		tableName := partition.TableName
		if !dbMap[dbName] {
			log.Println("skipping", dbName, ".", tableName, "db is not available")
			continue
		}
		fullTblName := dbName + "." + tableName
		if !tableMap[fullTblName] {
			_, err := client.GetTable(dbName, tableName)
			if err != nil {
				log.Println("skipping", dbName, ".", tableName, ": can't get table information")
				continue
			}
			tableMap[fullTblName] = true
		}
		if _, err := client.AddPartition(partition); err != nil {
			if _, ok := err.(*hive_metastore.AlreadyExistsException); ok {
				log.Println("failed to add partition", partition.Values, "into",
					fullTblName, ": partition already exists")

			} else {
				log.Println("failed to add partition", partition.Values, "into", fullTblName, err)
			}
		}
	}
	return nil
}

// Get list of databases as a map to allow quick check whether DB exists
func getDatabases(client *hmsclient.MetastoreClient) (map[string]bool, error) {
	databases, err := client.GetAllDatabases()
	if err != nil {
		return nil, fmt.Errorf("failed to get list of databases: %s",
			err.Error())
	}
	// Map of existing database names
	dbMap := make(map[string]bool)
	for _, dbName := range databases {
		dbMap[dbName] = true
	}
	return dbMap, nil
}

func init() {
	rootCmd.AddCommand(importCmd)
}

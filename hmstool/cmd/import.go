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

const (
	optFileName = "file"
)

// dbCmd represents the db command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "import HMS data",
	Run:   importData,
}

func importData(cmd *cobra.Command, args []string) {
	fileName, err := cmd.Flags().GetString(optFileName)
	if err != nil || fileName == "" {
		log.Fatal("missing file name")
	}
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	err = doImport(client, fileName)
	if err != nil {
		log.Fatal(err)
	}
}

func doImport(client *hmsclient.MetastoreClient, fileName string) error {
	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("failed to read file%s: %s", fileName, err.Error())
	}
	var hms HmsObject
	err = json.Unmarshal(raw, &hms)
	if err != nil {
		return fmt.Errorf("failed to parse file%s: %s", fileName, err.Error())
	}
	err = importDb(client, hms.Databases)
	if err != nil {
		return fmt.Errorf("failed to import databases from %s: %s", fileName, err.Error())
	}
	err = importTables(client, hms.Tables)
	if err != nil {
		return fmt.Errorf("failed to import tables from %s: %s", fileName, err.Error())
	}
	err = importPartitions(client, hms.Partitions)
	if err != nil {
		return fmt.Errorf("failed to import partitions from %s: %s", fileName, err.Error())
	}
	return nil
}

func importDb(client *hmsclient.MetastoreClient, dbs []*hmsclient.Database) error {
	databases, err := client.GetAllDatabases()
	if err != nil {
		return fmt.Errorf("failed to get list of databases: %s",
			err.Error())
	}
	// Map of existing database names
	dbMap := make(map[string]bool)
	for _, dbName := range databases {
		dbMap[dbName] = true
	}

	for _, db := range dbs {
		if !dbMap[db.Name] {
			dbMap[db.Name] = true
			// There may be issues with re-using location, so drop our location
			db.Location = ""
			if err = client.CreateDatabase(db); err != nil {
				return fmt.Errorf("failed to create database %s: %s",
					db.Name, err.Error())
			}
		}
	}

	return nil
}

func importTables(client *hmsclient.MetastoreClient, tables []*hive_metastore.Table) error {
	databases, err := client.GetAllDatabases()
	if err != nil {
		return fmt.Errorf("failed to get list of databases: %s",
			err.Error())
	}
	// Map of existing database names
	dbMap := make(map[string]bool)
	for _, dbName := range databases {
		dbMap[dbName] = true
	}
	for _, table := range tables {
		dbName := table.DbName
		tableName := table.TableName
		if !dbMap[dbName] {
			log.Println("skipping", dbName, ".", tableName, "db is not available")
			continue
		}
		_, err := client.GetTable(dbName, tableName)
		if err != nil {
			table.TableType = hmsclient.TableTypeExternal.String()
			log.Println("Adding table", dbName, ".", tableName)
			err = client.CreateTable(table)
			if err != nil {
				return fmt.Errorf("failed to create table %s.%s: %s",
					dbName, tableName, err.Error())
			}
		} else {
			log.Println("skipping", dbName, ".", tableName, ": table exist already")
		}
	}
	return nil
}

func importPartitions(client *hmsclient.MetastoreClient, partitions []*hive_metastore.Partition) error {
	return nil
}

func init() {
	importCmd.PersistentFlags().StringP(optFileName, "f", "", "HMS dump name")
	rootCmd.AddCommand(importCmd)
}

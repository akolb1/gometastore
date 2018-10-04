package cmd

import (
	"fmt"
	"log"

	"github.com/akolb1/gometastore/hmsclient"
	"github.com/spf13/cobra"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export databases or tables in JSON format",
}

var exportDbCmd = &cobra.Command{
	Use:     "databases",
	Aliases: []string{"db"},
	Run:     dbExport,
	Short:   "export databases in JSON format",
}

var exportTablesCmd = &cobra.Command{
	Use:   "tables",
	Run:   tableExport,
	Short: "Export tables in JSON format",
}

func dbExport(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if len(args) == 0 {
		if dbName, _ := cmd.Flags().GetString(optDbName); dbName != "" {
			args = []string{dbName}
		}
	}
	hmsObject := new(HmsObject)
	dbNames := make(map[string]bool)

	for _, dbName := range args {
		if !dbNames[dbName] {
			dbNames[dbName] = true
			if err = exportDatabase(client, hmsObject, dbName, true); err != nil {
				fmt.Println(err)
			}
		}
	}
	displayObject(hmsObject)
}

func tableExport(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if len(args) == 0 {
		if table, _ := cmd.Flags().GetString(optTableName); table != "" {
			args = []string{table}
		}
	}
	hmsObject := new(HmsObject)
	// names of databases that we already stored
	dbNames := make(map[string]bool)

	for _, tableName := range args {
		dbName, tableName := getDbTableName(cmd, tableName)
		if !dbNames[dbName] {
			dbNames[dbName] = true
			err = exportDatabase(client, hmsObject, dbName, false)
			if err != nil {
				fmt.Println(err)
			}
		}
		exportTable(client, hmsObject, dbName, tableName, true)
	}
	displayObject(hmsObject)
}

func exportDatabase(client *hmsclient.MetastoreClient,
	hmsObject *HmsObject,
	dbName string, recurse bool) error {
	db, err := client.GetDatabase(dbName)
	if err != nil {
		return fmt.Errorf("failed to get database %s: %s",
			dbName, err.Error())
	}
	hmsObject.Databases = append(hmsObject.Databases, db)
	if !recurse {
		return nil
	}
	tableNames, err := client.GetAllTables(dbName)
	if err != nil {
		return fmt.Errorf("failed to get tables for %s: %s",
			dbName, err.Error())
	}
	for _, tableName := range tableNames {
		err = exportTable(client, hmsObject, dbName, tableName, recurse)
		if err != nil {
			return fmt.Errorf("failed to export tables for %s: %s",
				dbName, err.Error())
		}

	}
	return nil
}
func exportTable(client *hmsclient.MetastoreClient,
	hmsObject *HmsObject, dbName string, tableName string, recurse bool) error {
	table, err := client.GetTable(dbName, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table %s: %s", tableName, err.Error())
	}
	hmsObject.Tables = append(hmsObject.Tables, table)
	if recurse {
		err = exportPartitions(client, hmsObject, dbName, tableName)
		if err != nil {
			return err
		}
	}
	return nil
}

func exportPartitions(client *hmsclient.MetastoreClient,
	hmsObject *HmsObject, dbName string, tableName string) error {
	partitions, err := client.GetPartitions(dbName, tableName, -1)
	if err != nil {
		return fmt.Errorf("failed to get partitions for %s: %s",
			tableName, err.Error())
	}
	hmsObject.Partitions = append(hmsObject.Partitions, partitions...)
	return nil
}

func init() {
	exportCmd.AddCommand(exportDbCmd)
	exportCmd.AddCommand(exportTablesCmd)
	rootCmd.AddCommand(exportCmd)
}

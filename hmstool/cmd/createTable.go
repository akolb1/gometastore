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
	"strings"

	"github.com/akolb1/gometastore/hmsclient"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/spf13/cobra"
)

const (
	stringType    = "string" // HMS representation of string type
	optColumns    = "columns"
	optPartitions = "partitions"
)

var tableCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create Table",
	Run:   createTable,
}

func createTable(cmd *cobra.Command, args []string) {
	dbName, _ := cmd.Flags().GetString(optDbName)
	tableName, _ := cmd.Flags().GetString(optTableName)
	// handle dbname.tablename syntax
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		dbName = parts[0]
		tableName = parts[1]
	}
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Check whether table already exists
	tableNames, err := client.GetAllTables(dbName)
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range tableNames {
		if name == tableName {
			log.Fatalf("table %s.%s already exists\n", dbName, tableName)
		}
	}
	owner := getOwner()
	params := argsToParams(args)
	columns, _ := cmd.Flags().GetString(optColumns)
	partitions, _ := cmd.Flags().GetString(optPartitions)

	table := hmsclient.NewTableBuilder(dbName, tableName).
		WithOwner(owner).
		WithColumns(getSchema(columns)).
		WithPartitionKeys(getSchema(partitions)).
		WithParameters(params).
		Build()

	err = client.CreateTable(table)

	if err != nil {
		log.Fatal(err)
	}
}

// getSchema converts argument to list of field schemas.
// Schema is represented as name=type,.... If type is missing, "string" is assumed.
func getSchema(arg string) []hive_metastore.FieldSchema {
	// First split on commas
	if arg == "" {
		return nil
	}
	fields := strings.Split(arg, ",")
	if len(fields) == 0 {
		return nil
	}
	schema := make([]hive_metastore.FieldSchema, 0, len(fields))
	for _, s := range fields {
		name := s
		typ := stringType
		parts := strings.Split(s, "=")
		if len(parts) == 2 {
			name = parts[0]
			typ = parts[1]
		}
		schema = append(schema, hive_metastore.FieldSchema{Name: name, Type: typ})
	}
	return schema
}

func init() {
	tableCreateCmd.Flags().StringP(optColumns, "C", "",
		"table columns separated by comma")
	tableCreateCmd.Flags().StringP(optPartitions, "P", "",
		"table partitions separated by comma")
	tablesCmd.AddCommand(tableCreateCmd)
}

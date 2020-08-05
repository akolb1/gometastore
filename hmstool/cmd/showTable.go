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

	"github.com/terry-sm/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/terry-sm/gometastore/hmstool/hmsutil"
	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var tableShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show tables",
	Long:  "Show detailed table information in JSON format",
	Run:   showTables,
}

func showTables(cmd *cobra.Command, args []string) {
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
	listFiles, _ := cmd.Flags().GetBool(optFiles)
	timestamp, _ := cmd.Flags().GetInt(optTimeStamp)

	tables := make([]*hive_metastore.Table, 0, len(args))
	for _, tableName := range args {
		dbName, tableName := getDbTableName(cmd, tableName)
		table, err := client.GetTable(dbName, tableName)
		if err != nil {
			log.Fatalf("failed to get table information for %s.%s: %v",
				dbName, tableName, err)
		}
		if timestamp == 0 {
			tables = append(tables, table)
		} else if table.CreateTime <= int32(timestamp) {
			tables = append(tables, table)
		}
	}

	if listFiles {
		displayTableFiles(tables)
	} else {
		displayObject(&HmsObject{Tables: tables})
	}
}

func displayTableFiles(tables []*hive_metastore.Table) {
	for _, table := range tables {
		location := table.Sd.Location
		files, err := hmsutil.ListFiles(location)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s.%s: %s\n", table.DbName, table.TableName, location)
		for _, name := range files {
			fmt.Printf("\t%s/%s\n", location, name)
		}
	}
}

func init() {
	tableShowCmd.PersistentFlags().Bool(optFiles, false, "show files in a table")
	tablesCmd.AddCommand(tableShowCmd)
}

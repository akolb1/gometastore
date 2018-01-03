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
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/akolb1/gometastore/hmsclient"
	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var tableShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show tables",
	Run:   showTables,
}

func showTables(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	dbName, _ := cmd.Flags().GetString(optDbName)
	if len(args) == 0 {
		table, _ := cmd.Flags().GetString(optTableName)
		if table != "" {
			args = []string{table}
		}
	}
	for _, tableName := range args {
		showTable(client, dbName, tableName)
	}
}

// showTable shows JSON representation of HMS table.
func showTable(client *hmsclient.MetastoreClient, dbName string, tableName string) {
	// handle dbname.tablename syntax
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		dbName = parts[0]
		tableName = parts[1]
	}
	table, err := client.GetTable(dbName, tableName)
	if err != nil {
		fmt.Println(err)
		return
	}
	b, err := json.MarshalIndent(table, "", "  ")
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}
	fmt.Println(string(b))
}

func init() {
	tablesCmd.AddCommand(tableShowCmd)
}

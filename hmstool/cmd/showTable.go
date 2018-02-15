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
	"io/ioutil"
	"log"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	if len(args) == 0 {
		table, _ := cmd.Flags().GetString(optTableName)
		if table != "" {
			args = []string{table}
		}
	}
	values := make([]interface{}, len(args))
	for i, tableName := range args {
		dbName, tableName := getDbTableName(cmd, tableName)
		values[i], err = client.GetTable(dbName, tableName)
		if err != nil {
			log.Fatalf("failed to get table information for %s.%s: %v",
				dbName, tableName, err)
		}
	}
	hmsObject := HmsObject{
		Type:   tableType,
		Values: values,
	}
	b, err := json.MarshalIndent(hmsObject, "", "  ")
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}
	outputFileName := viper.GetString(outputOpt)
	if outputFileName == "" {
		fmt.Println(string(b))
	} else {
		if err := ioutil.WriteFile(outputFileName, b, 0644); err != nil {
			log.Println("failed to write data to file", outputFileName, err)
		}
	}
}

func init() {
	tablesCmd.AddCommand(tableShowCmd)
}

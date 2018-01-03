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
	"strings"

	"github.com/spf13/cobra"
)

const (
	optTableName = "table"
)

// tablesCmd represents the tables command
var tablesCmd = &cobra.Command{
	Use:   "table",
	Short: "table operations",
}

// getDbTableName gets DB name and table name from input string.
// String can be dbName.tableName or just tableName.
func getDbTableName(cmd *cobra.Command, arg string) (dbName string, tableName string) {
	dbName, _ = cmd.Flags().GetString(optDbName)
	tableName = arg
	if tableName == "" {
		tableName, _ = cmd.Flags().GetString(optTableName)
	}
	parts := strings.Split(arg, ".")
	if len(parts) == 2 {
		dbName = parts[0]
		tableName = parts[1]
	}
	return dbName, tableName
}

func init() {
	tablesCmd.AddCommand(showPartitionsCmd)
	tablesCmd.AddCommand(showPartitionCmd)
	rootCmd.AddCommand(tablesCmd)

	tablesCmd.PersistentFlags().StringP(optDbName, "d", "default", "database name")
	tablesCmd.PersistentFlags().StringP(optTableName, "t", "", "table name")
}

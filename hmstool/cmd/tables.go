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

	"log"

	"github.com/spf13/cobra"
)

const (
	optTableName = "table"
)

// tablesCmd represents the tables command
var tablesCmd = &cobra.Command{
	Use:   "table",
	Short: "table operations",
	Long: `Operations on HMS tables. All operations need the host name which can be specified with -H flag or with
HMS_HOST environment variable.
Some commands require database name. Database can be provided either using '-d' flag or
deducted from table name which can be of the form 'dbName.tableName''. Some commands will get table names from
HMS if it isn't specified.

For listing tables two filtering options are supported. Client-side filtering is used by 'list' command
and server-side filtering is done by 'select command'.
`,
}

var tableDropCmd = &cobra.Command{
	Use:   "drop",
	Short: "drop table",
	Run:   dropTable,
	Long: `drop Give table. The table can be specified with '-t' flag or as the single argument.
The database can be specified with '-d' flag or iwth the table name which can be of the form
'dbName.tableName'.

    Example:

  hmstool table drop default.foo
  hmstool table drop -d default foo
  hmstool table drop -d default -t foo
`,
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

func dropTable(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	arg := ""
	if len(args) == 0 {
		arg = args[0]
	}
	dbName, tableName := getDbTableName(cmd, arg)
	if dbName == "" {
		log.Fatalln("missing database name")
	}
	if tableName == "" {
		log.Fatalln("missing table name")
	}
	err = client.DropTable(dbName, tableName, true)
	if err != nil {
		log.Fatalf("failed to drop table %s.%s: %v\n", dbName, tableName, err)
	}

}

func init() {
	tablesCmd.AddCommand(showPartitionsCmd)
	tablesCmd.AddCommand(showPartitionCmd)
	tablesCmd.AddCommand(tableDropCmd)
	rootCmd.AddCommand(tablesCmd)

	tablesCmd.PersistentFlags().StringP(optDbName, "d", "", "database name")
	tablesCmd.PersistentFlags().StringP(optTableName, "t", "", "table name")
}

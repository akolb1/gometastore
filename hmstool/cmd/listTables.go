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
	"strings"

	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var tableListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "list tables",
	Run:     listTables,
	Long:   `List tables matching specified pattern. By default list all table names.

The pattern can be specified in two ways and it affects the way it is applied.
It can be just added on the command line in which case all table names are fetched from HMS
and glob style matching is applied. Alternatively, if the pattern is specified with -M flag,
the glob pattern is passed to the server. This can be useful when there are a lot of tables.

Examples:

    hmstool table list -d default "*customer"
    hmstool table list -d default -M "*customer"

Both of these commands will show all table names in the default database 
which have customer in their name, but the first one will use client-side
matching and the second one will use server-side matching.
`,
}

// dbCmd represents the db command
var tableSelectCmd = &cobra.Command{
	Use:   "select",
	Short: "select tables using server-side filtering",
	Run:   selectTables,
}

func listTables(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	var dbNames []string

	if dbName, _ := cmd.Flags().GetString(optDbName); dbName != "" {
		dbNames = []string{dbName}
	} else {
		databases, err := client.GetAllDatabases()
		if err != nil {
			log.Fatal(err)
		}
		dbNames = databases
	}

	var tables []string
	for _, d := range dbNames {
		var tableList []string
		if pattern, _ := cmd.Flags().GetString("match"); pattern != "" {
			tableList, err = client.GetTables(d, pattern)
		} else {
			tableList, err = client.GetAllTables(d)
		}

		if err != nil {
			log.Fatal(err)
		}
		for _, t := range tableList {
			tables = append(tables, d+"."+t)
		}
	}

	if len(args) == 0 {
		args = []string{"*.*"}
	}
	var filteredTables []string
	globs := make([]glob.Glob, len(args))
	for i, a := range args {
		globs[i] = glob.MustCompile(a)
	}
	for _, t := range tables {
		for _, g := range globs {
			if g.Match(t) {
				filteredTables = append(filteredTables, t)
				break
			}
		}
	}

	for _, t := range filteredTables {
		fmt.Println(t)
	}
}

// selectTables finds tables using server-side filtering
func selectTables(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	dbName, _ := cmd.Flags().GetString(optDbName)
	if dbName == "" {
		dbName = "*"
	}
	tableName, _ := cmd.Flags().GetString(optTableName)
	if tableName == "" {
		tableName = "*"
	}
	// convert args to upper case
	upcaseArgs := make([]string, len(args))
	for i, a := range args {
		upcaseArgs[i] = strings.ToUpper(a)
	}
	tableData, err := client.GetTableMeta(dbName, tableName, upcaseArgs)
	if err != nil {
		log.Fatalln(err)
	}
	for _, t := range tableData {
		fmt.Printf("%s.%s\n", t.DbName, t.TableName)
	}
}

func init() {
	tableListCmd.Flags().BoolP("long", "l", false, "show table info")
	tableListCmd.Flags().StringP("match", "M", "", "only return tables matching pattern")
	tablesCmd.AddCommand(tableListCmd)
	tablesCmd.AddCommand(tableSelectCmd)
}

// Copyright © 2017 Alex Kolbasov
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

	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var tableListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "list tables",
	Run:     listTables,
}

func listTables(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	dbName, _ := cmd.Flags().GetString(optDbName)

	var dbNames []string
	if dbName != "" {
		dbNames = []string{dbName}
	} else {
		databases, err := client.GetAllDatabases()
		if err != nil {
			log.Fatal(err)
		}
		dbNames = databases
	}

	tables := []string{}
	for _, d := range dbNames {
		tableList, err := client.GetAllTables(dbName)
		if err != nil {
			log.Fatal(err)
		}
		for _, t := range tableList {
			tables = append(tables, d+"."+t)
		}
	}

	filteredTables := []string{}
	if len(args) == 0 {
		filteredTables = tables
	} else {
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
	}

	for _, t := range filteredTables {
		fmt.Println(t)
	}
}

func init() {
	tableListCmd.Flags().BoolP("long", "l", false, "show table info")
	tablesCmd.AddCommand(tableListCmd)
}
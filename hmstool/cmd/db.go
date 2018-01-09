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

	"github.com/spf13/cobra"
)

const (
	optDbName = "dbname"
)

// dbCmd represents the db command
var dbCmd = &cobra.Command{
	Use:              "db",
	Short:            "HMS database operations",
	TraverseChildren: true,
}

var dbDropCmd = &cobra.Command{
	Use:   "drop",
	Short: "drop database",
	Long:  "drop database db1, ...",
	Run:   dropDB,
}

func dropDB(cmd *cobra.Command, args []string) {
	dbName, _ := cmd.Flags().GetString(optDbName)
	var dbNames []string
	if dbName != "" && dbName != "default" {
		dbNames = []string{dbName}
	} else {
		dbNames = args
	}
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	for _, dbName = range dbNames {
		log.Println("Dropping database", dbName)
		if dbName == "default" {
			log.Println("skipping default database")
			continue
		}
		if err = client.DropDatabase(dbName, true, true); err != nil {
			log.Println("failed to delete", dbName, err)
		}
	}
}

func init() {
	dbCmd.PersistentFlags().StringP(optDbName, "d", "default", "database name")
	dbCmd.AddCommand(dbDropCmd)
	rootCmd.AddCommand(dbCmd)
}

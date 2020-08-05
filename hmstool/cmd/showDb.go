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

	"github.com/terry-sm/gometastore/hmsclient"
	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var dbShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show HMS database",
	Run:   showDB,
}

func showDB(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	if len(args) == 0 {
		if dbName, _ := cmd.Flags().GetString(optDbName); dbName != "" {
			args = append(args, dbName)
		}
	}
	dbs := make([]*hmsclient.Database, len(args))
	for i, a := range args {
		dbs[i], err = client.GetDatabase(a)
		if err != nil {
			log.Printf("failed to get database %s: %v", a, err)
		}
	}
	displayObject(&HmsObject{Databases: dbs})
}

func init() {
	dbCmd.AddCommand(dbShowCmd)
}

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

	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var dbShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show HMS database",
	Long:  `Show HMS database`,
	Run:   showDB,
}

func init() {
	dbCmd.AddCommand(dbShowCmd)
}

func showDB(_ *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	for _, a := range args {
		db, derr := client.GetDatabase(a)
		if derr != nil {
			log.Printf("failed to get database %s: %v", a, derr)
		}
		fmt.Printf("%s: %s\n", db.Name, db.Location)
		if len(db.Description) > 0 {
			fmt.Println("\t", db.Description)
		}
		for k, v := range db.Parameters {
			fmt.Println("\n\t", k, ": ", v)
		}
	}
}

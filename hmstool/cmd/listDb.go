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

	"github.com/gobwas/glob"
	"github.com/spf13/cobra"
)

// dbCmd represents the db command
var dbListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "list databases",
	Run:     listDbs,
}

func listDbs(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	databases, err := client.GetAllDatabases()
	if err != nil {
		log.Fatal(err)
	}

	dbNames := []string{}

	if len(args) == 0 {
		dbNames = databases
	} else {
		globs := make([]glob.Glob, len(args))
		for i, a := range args {
			globs[i] = glob.MustCompile(a)
		}
		for _, d := range databases {
			for _, g := range globs {
				if g.Match(d) {
					dbNames = append(dbNames, d)
					break
				}
			}
		}
	}

	if isLong, _ := cmd.Flags().GetBool("long"); !isLong {
		for _, name := range dbNames {
			fmt.Println(name)
		}
	} else {
		showDB(cmd, dbNames)
	}

}

func init() {
	dbListCmd.Flags().BoolP("long", "l", false, "show db info")
	dbCmd.AddCommand(dbListCmd)
}

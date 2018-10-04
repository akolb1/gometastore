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
	Long: `List databases matching specified pattern. By default list all database names.

The pattern can be specified in two ways and it affects the way it is applied.
It can be just added on the command line in which case all databse names are fetched from HMS
and glob style matching is applied. Alternatively, if the pattern is specified with -M flag,
the glob pattern is passed to the server. This can be useful when there are a lot of databases.

Examples:

    hmstool db list "*customer"
    hmstool db list -M "*customer"

Both of these commands will show all database names which have customer in their name,
but the first one will use client-side matching and the second one will use server-side matching.

`,
}

func listDbs(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var databases []string

	if pattern, _ := cmd.Flags().GetString("match"); pattern != "" {
		databases, err = client.GetDatabases(pattern)
	} else {
		databases, err = client.GetAllDatabases()
	}

	if err != nil {
		log.Fatal(err)
	}

	var dbNames []string

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
	dbListCmd.Flags().StringP("match", "M", "", "only return databases matching pattern")
	dbCmd.AddCommand(dbListCmd)
}

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

import "github.com/spf13/cobra"

// dbCmd represents the db command
var showCmd = &cobra.Command{
	Use:     "show",
	Aliases: []string{"ls"},
	Short:   "Show objects",
}

var showDbCmd = &cobra.Command{
	Use:     "database",
	Aliases: []string{"db"},
	Short:   "show database",
	Run:     listDbs,
}

var showTableCmd = &cobra.Command{
	Use:   "table",
	Short: "show table",
	Run:   listTables,
}

func init() {
	showCmd.PersistentFlags().StringP(optDbName, "d", "default", "database name")
	showCmd.AddCommand(showDbCmd)
	showCmd.AddCommand(showTableCmd)
	rootCmd.AddCommand(showCmd)
}

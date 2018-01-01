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
var describeCmd = &cobra.Command{
	Use:     "describe",
	Aliases: []string{"ls"},
	Short:   "describe objects",
}

var describeDbCmd = &cobra.Command{
	Use:     "database",
	Aliases: []string{"db"},
	Short:   "describe database",
	Run:     showDB,
}

var describeTableCmd = &cobra.Command{
	Use:   "table",
	Short: "describe table",
	Run:   showTables,
}

func init() {
	describeCmd.PersistentFlags().StringP(optDbName, "d", "default", "database name")
	describeCmd.AddCommand(describeDbCmd)
	describeCmd.AddCommand(describeTableCmd)
	rootCmd.AddCommand(describeCmd)
}

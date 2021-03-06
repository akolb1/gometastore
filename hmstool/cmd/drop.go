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

import "github.com/spf13/cobra"

var dropCmd = &cobra.Command{
	Use:   "drop",
	Short: "drop objects",
}

var dropDbCmd = &cobra.Command{
	Use:   "database",
	Short: "drop database",
	Run:   dropDB,
}

var dropTableCmd = &cobra.Command{
	Use:   "table",
	Short: "drop table",
	Run:   dropTable,
}

func init() {
	dropCmd.PersistentFlags().StringP(optDbName, "d", "default", "database name")
	dropCmd.AddCommand(dropDbCmd)
	dropCmd.AddCommand(dropTableCmd)
	rootCmd.AddCommand(dropCmd)
}

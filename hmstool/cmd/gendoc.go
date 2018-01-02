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
	"github.com/spf13/cobra/doc"
)

// dbCmd represents the db command
var docCmd = &cobra.Command{
	Use:              "doc",
	Short:            "Generate documentation",
	TraverseChildren: true,
	Run:              genDoc,
}

func genDoc(cmd *cobra.Command, args []string) {
	docdir, _ := cmd.Flags().GetString("dir")
	err := doc.GenMarkdownTree(rootCmd, docdir)
	if err != nil {
		log.Fatal(err)
	}
}

func init() {
	docCmd.Flags().StringP("dir", "d", "./doc", "doc directory")
	rootCmd.AddCommand(docCmd)
}

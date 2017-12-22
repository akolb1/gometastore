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
	"bytes"
	"fmt"
	"github.com/akolb1/gometastore/microbench"
	"github.com/spf13/cobra"
	"log"
)

func run(cmd *cobra.Command, args []string) {
	suite := microbench.MakeBenchmarkSuite(scale, true)
	client, err := getClient()
	if err != nil {
		log.Fatal("failed to connect to HMS:", err)
	}

	bd := makeBenchData(15, 100, "", client)
	suite.Add("listDababases",
		func() *microbench.Stats { return benchListDatabases(bd) })
	suite.Run()
	buf := new(bytes.Buffer)
	suite.Display(buf)
	fmt.Print(buf.String())
}

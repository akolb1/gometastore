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
	"bytes"
	"fmt"
	"log"

	"github.com/akolb1/gometastore/microbench"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func run(_ *cobra.Command, _ []string) {
	warmup := viper.GetInt(warmOpt)
	iterations := viper.GetInt(iterOpt)
	sanitize := viper.GetBool(sanitizeOpt)

	log.Println("Using warmup =", warmup, "iterations =", iterations,
		"sanitize =", sanitize)

	suite := microbench.MakeBenchmarkSuite(scale, sanitize)
	client, err := getClient()
	if err != nil {
		log.Fatal("failed to connect to HMS:", err)
	}

	bd := makeBenchData(warmup, iterations, "", client)
	suite.Add("listDababases",
		func() *microbench.Stats { return benchListDatabases(bd) })
	suite.Run()
	buf := new(bytes.Buffer)
	suite.Display(buf)
	fmt.Print(buf.String())
}

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

	"io/ioutil"

	"os"
	"path"

	"regexp"

	"github.com/akolb1/gometastore/microbench"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func run(_ *cobra.Command, _ []string) {
	warmup := viper.GetInt(warmOpt)
	iterations := viper.GetInt(iterOpt)
	sanitize := viper.GetBool(sanitizeOpt)
	dbName := viper.GetString(dbOpt)

	if dbName == "" {
		log.Fatal("missing database name")
	}

	log.Println("Using warmup =", warmup, "iterations =", iterations,
		"sanitize =", sanitize)

	suite := microbench.MakeBenchmarkSuite(scale, sanitize)
	client, err := getClient()
	if err != nil {
		log.Fatal("failed to connect to HMS:", err)
	}

	bd := makeBenchData(warmup, iterations, dbName, getOwner(), client)
	suite.Add("listDababases",
		func() *microbench.Stats { return benchListDatabases(bd) })
	suite.Add("getDatabase",
		func() *microbench.Stats { return benchGetDatabase(bd) })
	suite.Add("createDatabase",
		func() *microbench.Stats { return benchCreateDatabase(bd) })
	suite.Add("dropDatabase",
		func() *microbench.Stats { return benchDropDatabase(bd) })
	suite.Add("createTable",
		func() *microbench.Stats { return benchCreateTable(bd) })
	suite.Add("dropTable",
		func() *microbench.Stats { return benchDropTable(bd) })

	if viper.GetBool(listOpt) {
		// Only list benchmarks, don't run them
		for _, name := range suite.List() {
			fmt.Println(name)
		}
		return
	}

	if filter := viper.GetString(filterOpt); filter == "" {
		suite.Run()
	} else {
		var names []string
		for _, name := range suite.List() {
			if matched, _ := regexp.MatchString(filterOpt, name); matched {
				names = append(names, name)
			}
		}
		suite.RunSelected(names)
	}

	outputFileName := viper.GetString(outputOpt)
	buf := new(bytes.Buffer)
	if viper.GetBool(csvOpt) {
		suite.DisplayCSV(buf, "\t")
	} else {
		suite.Display(buf)
	}
	// Write output to selected destination
	if outputFileName == "" || outputFileName == "-" {
		// Print to stdout
		fmt.Print(buf.String())
	} else {
		if err := ioutil.WriteFile(outputFileName, buf.Bytes(), 0644); err != nil {
			log.Fatalf("failed to write to %s: %v", outputFileName, err)
		}
	}

	// Save raw data if requested
	saveLocation := viper.GetString(saveOpt)
	if saveLocation != "" {
		saveResults(suite.GetResults(), saveLocation)
	}
}

func saveResults(results map[string]*microbench.Stats, location string) {
	// if location doesn't exist, create it
	if err := os.MkdirAll(location, 0755); err != nil {
		log.Fatalln("failed to create directory", location, err)
	}
	for name, data := range results {
		buf := new(bytes.Buffer)
		data.Write(buf)
		dst := path.Join(location, name)
		if err := ioutil.WriteFile(dst, buf.Bytes(), 0644); err != nil {
			log.Println("failed to write data to file", dst, err)
		}
	}
}

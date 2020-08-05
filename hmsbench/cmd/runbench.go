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
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"

	"github.com/terry-sm/gometastore/hmsclient"
	"github.com/terry-sm/gometastore/microbench"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func run(_ *cobra.Command, _ []string) {
	warmup := viper.GetInt(warmOpt)
	iterations := viper.GetInt(iterOpt)
	sanitize := viper.GetBool(sanitizeOpt)
	dbName := viper.GetString(dbOpt)
	nObjects := viper.GetInt(objectsOpt)
	nThreads := viper.GetInt(threadOpt)

	if dbName == "" {
		log.Fatal("missing database name")
	}

	log.Println("Using warmup =", warmup, "iterations =", iterations, "nobjects =", nObjects,
		"sanitize =", sanitize)

	suite := microbench.MakeBenchmarkSuite(scale, sanitize)
	client, err := getClient()
	if err != nil {
		log.Fatal("failed to connect to HMS:", err)
	}

	if err := client.CreateDatabase(&hmsclient.Database{Name: dbName}); err != nil {
		log.Fatalf("failed to create database %s: %v", dbName, err)
	}
	defer client.DropDatabase(dbName, true, true)

	bd := makeBenchData(warmup, iterations, dbName, getOwner(), client, nObjects, nThreads)
	suite.Add("getNid",
		func() *microbench.Stats { return benchGetNotificationId(bd) })
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
	suite.Add(fmt.Sprintf("dropTable.%d", nObjects),
		func() *microbench.Stats { return benchDeleteTableWithPartitions(bd) })
	suite.Add("getTable",
		func() *microbench.Stats { return benchGetTable(bd) })
	suite.Add("addPartition",
		func() *microbench.Stats { return benchAddPartition(bd) })
	suite.Add("dropPartition",
		func() *microbench.Stats { return benchDropPartition(bd) })
	suite.Add("tableRename",
		func() *microbench.Stats { return benchTableRename(bd) })
	suite.Add(fmt.Sprintf("listTables.%d", nObjects),
		func() *microbench.Stats { return benchListManyTables(bd) })
	suite.Add(fmt.Sprintf("getTableObjectsByName.%d", nObjects),
		func() *microbench.Stats { return benchGetTableObjects(bd) })
	suite.Add(fmt.Sprintf("getPartitions.%d", nObjects),
		func() *microbench.Stats { return benchGetPartitions(bd) })
	suite.Add(fmt.Sprintf("addPartitions.%d", nObjects),
		func() *microbench.Stats { return benchCreatePartitions(bd) })
	suite.Add(fmt.Sprintf("dropPartitions.%d", nObjects),
		func() *microbench.Stats { return benchDropPartitions(bd) })
	suite.Add(fmt.Sprintf("dropPartitions.%d", nObjects),
		func() *microbench.Stats { return benchDropPartitions(bd) })
	suite.Add(fmt.Sprintf("concurrentPartsCreate#%d.%d", nThreads, nObjects),
		func() *microbench.Stats { return benchAddPartitionsInParallel(bd) })

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
			if matched, _ := regexp.MatchString(filter, name); matched {
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

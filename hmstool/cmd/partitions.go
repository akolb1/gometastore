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
	"strings"

	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/spf13/cobra"
)

const (
	maxParts = 500
)

func showPartitions(cmd *cobra.Command, args []string) {
	tableName := ""
	if len(args) != 0 {
		tableName = args[0]
	} else {
		tableName, _ = cmd.Flags().GetString(optTableName)
	}
	if tableName == "" {
		log.Fatal("missing table name")
	}
	dbName, _ := cmd.Flags().GetString(optDbName)
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		dbName = parts[0]
		tableName = parts[1]
	}
	if dbName == "" {
		log.Fatal("missing db name")
	}
	client, err := getClient()
	defer client.Close()
	if err != nil {
		log.Fatal(err)
	}
	partitions, err := client.GetPartitionNames(dbName, tableName, maxParts)
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range partitions {
		fmt.Println(p)
	}
}

func showPartition(cmd *cobra.Command, args []string) {
	tableName, _ := cmd.Flags().GetString(optTableName)
	if tableName == "" {
		log.Fatal("missing table name")
	}
	dbName, _ := cmd.Flags().GetString(optDbName)
	parts := strings.Split(tableName, ".")
	if len(parts) == 2 {
		dbName = parts[0]
		tableName = parts[1]
	}
	if dbName == "" {
		log.Fatal("missing db name")
	}
	client, err := getClient()
	defer client.Close()
	if err != nil {
		log.Fatal(err)
	}
	// partitions := make([]*hive_metastore.Partition, len(args))
	partitions := make([]*hive_metastore.Partition, len(args))
	for i, arg := range args {
		partitions[i], err = client.GetPartitionByName(dbName, tableName, arg)
		if err != nil {
			log.Fatalf("can not get partition %s: %v", arg, err)
		}
	}
	displayObject(&HmsObject{Partitions: partitions})
}

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

	"github.com/terry-sm/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/terry-sm/gometastore/hmstool/hmsutil"
	"github.com/spf13/cobra"
)

const (
	maxParts = 500
)

var partitionsCmd = &cobra.Command{
	Use:   "partitions",
	Short: "partitions operations",
}

var partitionsListCmd = &cobra.Command{
	Use:   "list",
	Short: "list partitions",
	Run:   showPartitions,
}

var partitionShowCmd = &cobra.Command{
	Use:   "show",
	Short: "show partition",
	Run:   showPartition,
}

var partitionDropCmd = &cobra.Command{
	Use:   "drop",
	Short: "drop partition",
	Run:   dropPartition,
}

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

	timestamp, _ := cmd.Flags().GetInt(optTimeStamp)

	// partitions := make([]*hive_metastore.Partition, len(args))
	partitions := make([]*hive_metastore.Partition, 0, len(args))
	for _, arg := range args {
		part, err := client.GetPartitionByName(dbName, tableName, arg)
		if err != nil {
			log.Fatalf("can not get partition %s: %v", arg, err)
		}
		if timestamp == 0 {
			partitions = append(partitions, part)
		} else if part.CreateTime <= int32(timestamp) {
			partitions = append(partitions, part)
		}
	}
	listFiles, _ := cmd.Flags().GetBool(optFiles)
	if listFiles {
		displayPartitionFiles(partitions)
	} else {
		displayObject(&HmsObject{Partitions: partitions})
	}
}

func dropPartition(cmd *cobra.Command, args []string) {
	tableName, _ := cmd.Flags().GetString(optTableName)
	if tableName == "" {
		log.Fatal("missing table name")
	}
	if len(args) == 0 {
		log.Fatal("no partitions to drop")
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
	var values []string
	for _, arg := range args {
		parts := strings.Split(arg, "=")
		value := parts[0]
		if len(parts) > 1 {
			value = parts[1]
		}
		values = append(values, value)
	}
	_, err = client.DropPartition(dbName, tableName, values, true)
	if err != nil {
		fmt.Println(err)
	}
}

// List partition location and all files in a partition
func displayPartitionFiles(partitions []*hive_metastore.Partition) {
	for _, part := range partitions {
		location := part.Sd.Location
		files, err := hmsutil.ListFiles(location)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: \t%s\n", strings.Join(part.Values, "/"), location)
		for _, name := range files {
			fmt.Printf("\t%s/%s\n", location, name)
		}
	}
}

func init() {
	partitionsCmd.PersistentFlags().StringP(optDbName, "d", "", "database name")
	partitionsCmd.PersistentFlags().StringP(optTableName, "t", "", "table name")
	partitionsCmd.PersistentFlags().Bool(optFiles, false, "show files in a partition")
	partitionsCmd.PersistentFlags().Int(optTimeStamp, 0, "timestamp")
	partitionsCmd.AddCommand(partitionsListCmd)
	partitionsCmd.AddCommand(partitionShowCmd)
	partitionsCmd.AddCommand(partitionDropCmd)
	rootCmd.AddCommand(partitionsCmd)
}

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

package hmsclient

import (
	"fmt"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"strings"
)

const (
	defaultSerDe        = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	defaultInputFormat  = "org.apache.hadoop.mapred.TextInputFormat"
	defaultOutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
)

// convertSchema converts list of FieldSchema to list of pointers to FieldSchema
func convertSchema(columns []hive_metastore.FieldSchema) []*hive_metastore.FieldSchema {
	var cols []*hive_metastore.FieldSchema
	if len(columns) > 0 {
		cols = make([]*hive_metastore.FieldSchema, len(columns))
		for i, c := range columns {
			cols[i] = &c
			// Type defaults to string
			if cols[i].Type == "" {
				cols[i].Type = "string"
			}
		}
	}
	return cols
}

// MakeTable returns initialized Table object.
// Parameters:
//   dbName  database name
//   tableName  table name
//   owner Table owner
//   parameters Table parameters
//   columns list of table column descriptions
//   partitions list of table partitions descriptions
func MakeTable(dbName string, tabeName string, owner string,
	parameters map[string]string,
	columns []hive_metastore.FieldSchema,
	partitions []hive_metastore.FieldSchema) *hive_metastore.Table {

	// Create storage descriptor
	sd := &hive_metastore.StorageDescriptor{
		InputFormat:  defaultInputFormat,
		OutputFormat: defaultOutputFormat,
		Cols:         convertSchema(columns),
		SerdeInfo: &hive_metastore.SerDeInfo{
			Name:             tabeName,
			SerializationLib: defaultSerDe,
		},
	}
	return &hive_metastore.Table{
		DbName:        dbName,
		TableName:     tabeName,
		Owner:         owner,
		Sd:            sd,
		Parameters:    parameters,
		PartitionKeys: convertSchema(partitions),
	}
}

// MakePartition creates Partition object from ordere4d list of partition values.
// Parameters:
//   table - Hive table for which partition is added
//   values - List of partition values which should match partition schema
//   Only string values are currently supported
func MakePartition(table *hive_metastore.Table,
	values []string, parameters map[string]string) (*hive_metastore.Partition, error) {
	partitionKeys := table.PartitionKeys
	if len(partitionKeys) != len(values) {
		return nil, fmt.Errorf("number of provided partition values %d does not match partition"+
			" schema which has %d columns",
			len(values), len(partitionKeys))
	}
	// Construct name=value list for each partition
	partNames := make([]string, len(partitionKeys))
	for i, p := range partitionKeys {
		partNames[i] = p.Name + "=" + values[i]
	}
	sd := *table.Sd
	sd.Location = sd.Location + "/" + strings.Join(partNames, "/")
	return &hive_metastore.Partition{
		Values:     values,
		DbName:     table.DbName,
		TableName:  table.TableName,
		Sd:         &sd,
		Parameters: parameters,
	}, nil
}

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

package hmsclient

import (
	"fmt"
	"strings"

	"github.com/terry-sm/gometastore/hmsclient/thrift/gen-go/hive_metastore"
)

const (
	defaultSerDe        = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	defaultInputFormat  = "org.apache.hadoop.mapred.TextInputFormat"
	defaultOutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
)

// TableBuilder provides builder pattern for table objects
type TableBuilder struct {
	Db            string
	Name          string
	Type          TableType
	Serde         string
	Owner         string
	InputFormat   string
	OutputFormat  string
	Location      string
	Columns       []hive_metastore.FieldSchema
	PartitionKeys []hive_metastore.FieldSchema
	Parameters    map[string]string
}

type PartitionBuilder struct {
	Table      *hive_metastore.Table
	Parameters map[string]string
	Values     []string
	Location   string
}

// convertSchema converts list of FieldSchema to list of pointers to FieldSchema
func convertSchema(columns []hive_metastore.FieldSchema) []*hive_metastore.FieldSchema {
	if len(columns) == 0 {
		return nil
	}
	var cols []*hive_metastore.FieldSchema
	for _, c := range columns {
		col := c
		if col.Type == "" {
			col.Type = "string"
		}
		cols = append(cols, &col)
	}
	return cols
}

// Build HMS Table object.
func (tb *TableBuilder) Build() *hive_metastore.Table {
	return &hive_metastore.Table{
		DbName:        tb.Db,
		TableName:     tb.Name,
		Owner:         tb.Owner,
		Parameters:    tb.Parameters,
		TableType:     tb.Type.String(),
		PartitionKeys: convertSchema(tb.PartitionKeys),
		Sd: &hive_metastore.StorageDescriptor{
			InputFormat:  tb.InputFormat,
			OutputFormat: tb.OutputFormat,
			Location:     tb.Location,
			Cols:         convertSchema(tb.Columns),
			SerdeInfo: &hive_metastore.SerDeInfo{
				Name:             tb.Name,
				SerializationLib: tb.Serde,
			},
		},
	}
}

func NewTableBuilder(db string, tableName string) *TableBuilder {
	return &TableBuilder{
		Db:           db,
		Name:         tableName,
		Type:         TableTypeManaged,
		Serde:        defaultSerDe,
		InputFormat:  defaultInputFormat,
		OutputFormat: defaultOutputFormat,
	}
}

// WithOwner specifies table owner
func (tb *TableBuilder) WithOwner(owner string) *TableBuilder {
	tb.Owner = owner
	return tb
}

// WithParameter adds table parameter
func (tb *TableBuilder) WithParameter(name string, value string) *TableBuilder {
	if tb.Parameters == nil {
		tb.Parameters = make(map[string]string)
	}
	tb.Parameters[name] = value
	return tb
}

// WithParameters specifies table parameters
func (tb *TableBuilder) WithParameters(parameters map[string]string) *TableBuilder {
	tb.Parameters = parameters
	return tb
}

// WithType specifies table type
func (tb *TableBuilder) WithType(t TableType) *TableBuilder {
	if t == TableTypeExternal {
		tb.AsExternal()
	}
	return tb
}

// WithSerde specifies table serde
func (tb *TableBuilder) WithSerde(serde string) *TableBuilder {
	tb.Serde = serde
	return tb
}

// WithInputFormat specifies table input format
func (tb *TableBuilder) WithInputFormat(format string) *TableBuilder {
	tb.InputFormat = format
	return tb
}

// WithOutputFormat specifies table output format
func (tb *TableBuilder) WithOutputFormat(format string) *TableBuilder {
	tb.OutputFormat = format
	return tb
}

// WithLocation specifies table location
func (tb *TableBuilder) WithLocation(location string) *TableBuilder {
	tb.Location = location
	return tb
}

// WithColumns specifies table columns
func (tb *TableBuilder) WithColumns(columns []hive_metastore.FieldSchema) *TableBuilder {
	tb.Columns = columns
	return tb
}

// WithPartitionKeys specifies table partition keys
func (tb *TableBuilder) WithPartitionKeys(partKeys []hive_metastore.FieldSchema) *TableBuilder {
	tb.PartitionKeys = partKeys
	return tb
}

// Mark table as external
func (tb *TableBuilder) AsExternal() *TableBuilder {
	return tb.WithParameter("EXTERNAL", "true")
}

func (p *PartitionBuilder) Build() *hive_metastore.Partition {
	values := p.Values
	partitionKeys := p.Table.PartitionKeys
	sd := *p.Table.Sd
	if p.Location != "" {
		sd.Location = p.Location
	} else {
		// Construct name=value list for each partition
		partNames := make([]string, len(partitionKeys))
		for i, p := range partitionKeys {
			partNames[i] = p.Name + "=" + values[i]
		}
		sd.Location = sd.Location + "/" + strings.Join(partNames, "/")
	}
	return &hive_metastore.Partition{
		Values:     values,
		DbName:     p.Table.DbName,
		TableName:  p.Table.TableName,
		Sd:         &sd,
		Parameters: p.Parameters,
	}
}

func NewPartitionBuilder(table *hive_metastore.Table, values []string) (*PartitionBuilder, error) {
	if len(table.PartitionKeys) != len(values) {
		return nil, fmt.Errorf("number of provided partition values %d does not match partition"+
			" schema which has %d columns",
			len(values), len(table.PartitionKeys))
	}
	return &PartitionBuilder{Table: table, Values: values}, nil
}

func (pb *PartitionBuilder) WithLocation(location string) *PartitionBuilder {
	pb.Location = location
	return pb
}

func (pb *PartitionBuilder) WithParameters(parameters map[string]string) *PartitionBuilder {
	pb.Parameters = parameters
	return pb
}

func (pb *PartitionBuilder) WithParameter(key string, value string) *PartitionBuilder {
	if pb.Parameters == nil {
		pb.Parameters = make(map[string]string)
	}
	pb.Parameters[key] = value
	return pb
}

// MakePartition creates Partition object from ordered list of partition values.
// Only string values are currently supported.
// Parameters:
//   table  - Hive table for which partition is added
//   values - List of partition values which should match partition schema
func MakePartition(table *hive_metastore.Table,
	values []string, parameters map[string]string,
	location string) (*hive_metastore.Partition, error) {
	if pb, err := NewPartitionBuilder(table, values); err != nil {
		return nil, err
	} else {
		return pb.
			WithLocation(location).
			WithParameters(parameters).
			Build(), nil
	}
}

package cmd

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/akolb1/gometastore/hmsclient"
)

func TestMakePartNames(t *testing.T) {
	prefix := "d"
	names := makePartNames(prefix, 10)
	for i, name := range names {
		if name != fmt.Sprintf("%s=%s%d", testPartitionSchema, prefix, i) {
			t.Errorf("invalid name %s", name)
		}
	}
}

func TestMakeManyPartitions(t *testing.T) {
	dbName := "db"
	tableName := "table"
	prefix := "d"
	location := "/home"
	table := hmsclient.NewTableBuilder(dbName, tableName).
		WithPartitionKeys(getSchema("date")).
		WithLocation(location).
		Build()

	partitions := makeManyPartitions(table, prefix, 5)
	for i, p := range partitions {
		if !reflect.DeepEqual(p.Values, []string{fmt.Sprintf("%s%d", prefix, i)}) {
			t.Errorf("invalid value %s", p.Values)
		}
		if p.Sd.Location != fmt.Sprintf("%s/%s=%s%d", location, testPartitionSchema, prefix, i) {
			t.Errorf("invalid location %s", p.Sd.Location)
		}
	}
}

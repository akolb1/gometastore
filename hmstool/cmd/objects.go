package cmd

import (
	"github.com/akolb1/gometastore/hmsclient"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
)

type HmsObject struct {
	Databases  []*hmsclient.Database       `json:"databases,omitempty"`
	Tables     []*hive_metastore.Table     `json:"tables,omitempty"`
	Partitions []*hive_metastore.Partition `json:"partitions,omitempty"`
}

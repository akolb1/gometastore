package client

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/akolb1/gometastore/thrift/gen-go/hive_metastore"
)

// GetProtocol returns Hive model protocol
func GetProtocol(t thrift.TTransport) thrift.TProtocol {
	return thrift.NewTBinaryProtocolTransport(t)
}

// sentryCLient represents client handle for Hive model
type metastoreClient struct {
	userName  string
	transport thrift.TTransport
	client *hive_metastore.ThriftHiveMetastoreClient
}
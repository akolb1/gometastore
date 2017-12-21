package client

import (
	"context"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/akolb1/gometastore/thrift/gen-go/hive_metastore"
	"net"
	"strconv"
)

// metastoreClient represents client handle to Hive
type MetastoreClient struct {
	transport thrift.TTransport
	client    *hive_metastore.ThriftHiveMetastoreClient
}

// Open opens connection to metastore
func Open(host string, port int) (*MetastoreClient, error) {
	trans, err := thrift.NewTSocket(net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("error resolving address %s: %v", host, err)
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(trans)
	oprot := protocolFactory.GetProtocol(trans)
	client := hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(iprot, oprot))
	if err1 := trans.Open(); err1 != nil {
		return nil, fmt.Errorf("failed to open connection to %s:%d: %v", host, port, err1)
	}
	return &MetastoreClient{trans, client}, nil
}

// Close closes connection to metastore
func (c *MetastoreClient) Close() {
	c.transport.Close()
}

func (c *MetastoreClient) GetAllDatabases() ([]string, error) {
	return c.client.GetAllDatabases(context.Background())
}

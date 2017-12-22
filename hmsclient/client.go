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
	"context"
	"fmt"
	"net"
	"strconv"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
)

// metastoreClient represents hmsclient handle to Hive
type MetastoreClient struct {
	transport thrift.TTransport
	client    *hive_metastore.ThriftHiveMetastoreClient
}

// Open connection to metastore
func Open(host string, port int) (*MetastoreClient, error) {
	trans, err := thrift.NewTSocket(net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("error resolving address %s: %v", host, err)
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	iprot := protocolFactory.GetProtocol(trans)
	oprot := protocolFactory.GetProtocol(trans)
	c := hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(iprot, oprot))
	if err1 := trans.Open(); err1 != nil {
		return nil, fmt.Errorf("failed to open connection to %s:%d: %v", host, port, err1)
	}
	return &MetastoreClient{trans, c}, nil
}

// Close closes connection to metastore
func (c *MetastoreClient) Close() {
	c.transport.Close()
}

func (c *MetastoreClient) GetAllDatabases() ([]string, error) {
	return c.client.GetAllDatabases(context.Background())
}

func (c *MetastoreClient) GetDatabase(dbName string) (*hive_metastore.Database, error) {
	return c.client.GetDatabase(context.Background(), dbName)
}

func (c *MetastoreClient) CreateDatabase(dbName string, descr string, owner string) error {
	db := &hive_metastore.Database{Name: dbName, Description: descr}
	if owner != "" {
		db.OwnerName = &owner
	}
	return c.client.CreateDatabase(context.Background(), db)
}

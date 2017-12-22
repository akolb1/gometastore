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

const (
	bufferSize = 1024 * 1024
)

// MetastoreClient represents client handle.
type MetastoreClient struct {
	context   context.Context
	transport thrift.TTransport
	client    *hive_metastore.ThriftHiveMetastoreClient
}

// Database is a container of other objects in Hive.
type Database struct {
	Name        string
	Description string
	Owner       string
	Location    string
	Parameters  map[string]string
}

// Open connection to metastore and return client handle.
func Open(host string, port int) (*MetastoreClient, error) {
	socket, err := thrift.NewTSocket(net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("error resolving address %s: %v", host, err)
	}
	transportFactory := thrift.NewTBufferedTransportFactory(bufferSize)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transport, err := transportFactory.GetTransport(socket)
	if err != nil {
		return nil, err
	}

	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	c := hive_metastore.NewThriftHiveMetastoreClient(thrift.NewTStandardClient(iprot, oprot))
	if err1 := transport.Open(); err1 != nil {
		return nil, fmt.Errorf("failed to open connection to %s:%d: %v", host, port, err1)
	}
	return &MetastoreClient{context: context.Background(), transport: transport, client: c}, nil
}

// Close connection to metastore.
// Handle can't be used once it is closed.
func (c *MetastoreClient) Close() {
	c.transport.Close()
}

// GetAllDatabases returns list of all Hive databases.
func (c *MetastoreClient) GetAllDatabases() ([]string, error) {
	return c.client.GetAllDatabases(c.context)
}

// GetDatabase returns detailed information about specified Hive database.
func (c *MetastoreClient) GetDatabase(dbName string) (*Database, error) {
	db, err := c.client.GetDatabase(c.context, dbName)
	if err != nil {
		return nil, err
	}
	return &Database{
		Name:        db.GetName(),
		Description: db.GetDescription(),
		Parameters:  db.GetParameters(),
		Location:    db.GetLocationUri(),
		Owner:       db.GetOwnerName(),
	}, nil
}

// CreateDatabase creates database with the specified name, description, parameters and owner.
func (c *MetastoreClient) CreateDatabase(db Database) error {
	database := &hive_metastore.Database{Name: db.Name, Description: db.Description, Parameters: db.Parameters}
	if db.Owner != "" {
		database.OwnerName = &db.Owner
	}
	return c.client.CreateDatabase(c.context, database)
}

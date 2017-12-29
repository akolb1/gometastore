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
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Owner       string            `json:"owner,omitempty"`
	Location    string            `json:"location"`
	Parameters  map[string]string `json:"parameters,omitempty"`
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
func (c *MetastoreClient) CreateDatabase(db *Database) error {
	database := &hive_metastore.Database{
		Name:        db.Name,
		Description: db.Description,
		Parameters:  db.Parameters,
	}
	if db.Owner != "" {
		database.OwnerName = &db.Owner
	}
	// Thrift defines location as non-optional, but it turns out that it is optional for writing
	// (in which case HMS uses its own default) but not for reading.
	// The underlying Thrift-generated code is modified by hand to allow for missing locationUri
	// field. Here we send nil as location URI when location is empty.
	if db.Location != "" {
		database.LocationUri = &db.Location
	}
	return c.client.CreateDatabase(c.context, database)
}

// DropDatabases removes the database specified by name
// Parameters:
//   - dbName database name
//   - deleteData - if true, delete data as well
//   - cascade - delete everything under the db if true
func (c *MetastoreClient) DropDatabase(dbName string, deleteData bool, cascade bool) error {
	return c.client.DropDatabase(c.context, dbName, deleteData, cascade)
}

// GetAllTables returns list of all table names for a given database
func (c *MetastoreClient) GetAllTables(dbName string) ([]string, error) {
	return c.client.GetAllTables(c.context, dbName)
}

// GetTable returns detailed information about the specified table
func (c *MetastoreClient) GetTable(dbName string, tableName string) (*hive_metastore.Table, error) {
	return c.client.GetTable(c.context, dbName, tableName)
}

// CreateTable Creates HMS table
func (c *MetastoreClient) CreateTable(table *hive_metastore.Table) error {
	return c.client.CreateTable(c.context, table)
}

// DropTable drops table.
// Parameters
//   dbName Database name
//   tableName Table name
//   deleteData if True, delete data as well
func (c *MetastoreClient) DropTable(dbName string, tableName string, deleteData bool) error {
	return c.client.DropTable(c.context, dbName, tableName, deleteData)
}

// GetPartitionNames returns list of partition names for a table.
func (c *MetastoreClient) GetPartitionNames(dbName string, tableName string, max int) ([]string, error) {
	return c.client.GetPartitionNames(c.context, dbName, tableName, int16(max))
}

// GetPartitionByName returns Partition for the given partition name.
func (c *MetastoreClient) GetPartitionByName(dbName string, tableName string,
	partName string) (*hive_metastore.Partition, error) {
	return c.client.GetPartitionByName(c.context, dbName, tableName, partName)
}

// AddPartition adds partition to Hive table.
func (c *MetastoreClient) AddPartition(partition *hive_metastore.Partition) (*hive_metastore.Partition, error) {
	return c.client.AddPartition(c.context, partition)
}

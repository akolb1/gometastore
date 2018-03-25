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
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
)

type TableType int

const (
	TableTypeManaged TableType = iota
	TableTypeExternal
	TableTypeView
	TableTypeIndex
)

// String representation of table types, consumed by Hive
var tableTypes = []string{
	"MANAGED_TABLE",
	"EXTERNAL_TABLE",
	"VIRTUAL_VIEW",
	"INDEX_TABLE",
}

const (
	bufferSize = 1024 * 1024
)

// MetastoreClient represents client handle.
type MetastoreClient struct {
	context   context.Context
	transport thrift.TTransport
	client    *hive_metastore.ThriftHiveMetastoreClient
	server    string
	port      int
}

// Database is a container of other objects in Hive.
type Database struct {
	Name        string                       `json:"name"`
	Description string                       `json:"description,omitempty"`
	Owner       string                       `json:"owner,omitempty"`
	OwnerType   hive_metastore.PrincipalType `json:"ownerType,omitempty"`
	Location    string                       `json:"location"`
	Parameters  map[string]string            `json:"parameters,omitempty"`
}

func (val TableType) String() string {
	return tableTypes[val]
}

// Open connection to metastore and return client handle.
func Open(host string, port int) (*MetastoreClient, error) {
	server := host
	portStr := strconv.Itoa(port)
	if strings.Contains(host, ":") {
		s, pStr, err := net.SplitHostPort(host)
		if err != nil {
			return nil, err
		}
		server = s
		portStr = pStr
	}

	socket, err := thrift.NewTSocket(net.JoinHostPort(server, portStr))
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
	return &MetastoreClient{
		context:   context.Background(),
		transport: transport,
		client:    c,
		server:    host,
		port:      port,
	}, nil
}

// Close connection to metastore.
// Handle can't be used once it is closed.
func (c *MetastoreClient) Close() {
	c.transport.Close()
}

// Clone metastore client and return a new client with its own connection to metastore.
func (c *MetastoreClient) Clone() (client *MetastoreClient, err error) {
	return Open(c.server, c.port)
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

	result := &Database{
		Name:        db.GetName(),
		Description: db.GetDescription(),
		Parameters:  db.GetParameters(),
		Location:    db.GetLocationUri(),
		Owner:       db.GetOwnerName(),
	}

	if db.OwnerType != nil {
		result.OwnerType = *db.OwnerType
	}

	return result, nil
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
	if db.OwnerType != 0 {
		database.OwnerType = &db.OwnerType
	}
	return c.client.CreateDatabase(c.context, database)
}

// DropDatabases removes the database specified by name
// Parameters:
//   dbName     - database name
//   deleteData - if true, delete data as well
//   cascade    - delete everything under the db if true
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
//   dbName     - Database name
//   tableName  - Table name
//   deleteData - if True, delete data as well
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

// GetPartitionsByNames returns multiple partitions specified by names.
func (c *MetastoreClient) GetPartitionsByNames(dbName string, tableName string,
	partNames []string) ([]*hive_metastore.Partition, error) {
	return c.client.GetPartitionsByNames(c.context, dbName, tableName, partNames)
}

// AddPartition adds partition to Hive table.
func (c *MetastoreClient) AddPartition(partition *hive_metastore.Partition) (*hive_metastore.Partition, error) {
	return c.client.AddPartition(c.context, partition)
}

// AddPartitions adds multipe partitions in a single call.
func (c *MetastoreClient) AddPartitions(newParts []*hive_metastore.Partition) error {
	_, err := c.client.AddPartitions(c.context, newParts)
	return err
}

// GetPartitions returns all (or up to maxCount partitions of a table.
func (c *MetastoreClient) GetPartitions(dbName string, tableName string,
	maxCunt int) ([]*hive_metastore.Partition, error) {
	return c.client.GetPartitions(c.context, dbName, tableName, int16(maxCunt))
}

// DropPartitionByName drops partition specified by name.
func (c *MetastoreClient) DropPartitionByName(dbName string,
	tableName string, partName string, dropData bool) (bool, error) {
	return c.client.DropPartitionByName(c.context, dbName, tableName, partName, dropData)
}

// DropPartition drops partition specified by values.
func (c *MetastoreClient) DropPartition(dbName string,
	tableName string, values []string, dropData bool) (bool, error) {
	return c.client.DropPartition(c.context, dbName, tableName, values, dropData)
}

// DropPartitions drops multiple partitions within a single table.
// Partitions are specified by names.
func (c *MetastoreClient) DropPartitions(dbName string,
	tableName string, partNames []string) error {
	dropRequest := hive_metastore.NewDropPartitionsRequest()
	dropRequest.DbName = dbName
	dropRequest.TblName = tableName
	dropRequest.Parts = &hive_metastore.RequestPartsSpec{Names: partNames}
	_, err := c.client.DropPartitionsReq(c.context, dropRequest)
	return err
}

// GetCurrentNotificationId returns value of last notification ID
func (c *MetastoreClient) GetCurrentNotificationId() (int64, error) {
	r, err := c.client.GetCurrentNotificationEventId(c.context)
	return r.EventId, err
}

// AlterTable modifies existing table with data from the new table
func (c *MetastoreClient) AlterTable(dbName string, tableName string,
	table *hive_metastore.Table) error {
	return c.client.AlterTable(c.context, dbName, tableName, table)
}

// GetNextNotification returns next available notification.
func (c *MetastoreClient) GetNextNotification(lastEvent int64,
	maxEvents int32) ([]*hive_metastore.NotificationEvent, error) {
	r, err := c.client.GetNextNotification(c.context,
		&hive_metastore.NotificationEventRequest{LastEvent: lastEvent, MaxEvents: &maxEvents})
	if err != nil {
		return nil, err
	}
	return r.Events, nil
}

// GetTableMeta returns list of tables matching specified search criteria.
// Parameters:
//  db - database name pattern
//  table - table name pattern
//  tableTypes - list of Table types - should be either TABLE or VIEW
func (c *MetastoreClient) GetTableMeta(db string,
	table string, tableTypes []string) ([]*hive_metastore.TableMeta, error) {
	return c.client.GetTableMeta(c.context, db, table, tableTypes)
}

// GetTablesByType returns list of tables matching specified search criteria.
// Parameters:
//  dbName - database name
//  table - table name pattern
//  tableType - Table type - should be either TABLE or VIEW
func (c *MetastoreClient) GetTablesByType(dbName string,
	table string, tableType string) ([]string, error) {
	return c.client.GetTablesByType(c.context, dbName, table, tableType)
}

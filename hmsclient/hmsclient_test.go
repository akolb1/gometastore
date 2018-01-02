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

package hmsclient_test

import (
	"fmt"
	"log"
	"testing"

	"os"

	"github.com/akolb1/gometastore/hmsclient"
)

func ExampleOpen() {
	client, err := hmsclient.Open("localhost", 9083)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(client.GetAllDatabases())
}

func ExampleMetastoreClient_GetAllDatabases() {
	client, err := hmsclient.Open("localhost", 9083)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	fmt.Println(client.GetAllDatabases())
}

func TestOpenBadHost(t *testing.T) {
	t.Log("connecting to fake host")
	client, err := hmsclient.Open("foobar", 1)
	if err == nil {
		t.Error("connection to bad host succeeded")
	}
	if client != nil {
		t.Error("connecting to bad host returned valid client")
	}
}

func getClient(t *testing.T) (*hmsclient.MetastoreClient, error) {
	host := os.Getenv("HMS_SERVER")
	if host == "" {
		host = "localhost"
	}
	t.Log("connecting to", host)
	client, err := hmsclient.Open(host, 9083)
	if err != nil {
		t.Error("failed connection to", host, err)
		return nil, err
	}
	return client, nil
}

func TestGetDatabases(t *testing.T) {
	client, err := getClient(t)
	if err != nil {
		return
	}
	defer client.Close()
	databases, err := client.GetAllDatabases()
	if err != nil {
		t.Error("failed to get databases", err)
		return
	}
	if len(databases) == 0 {
		t.Error("no databases available")
		return
	}
}

func TestMetastoreClient_CreateDatabase(t *testing.T) {
	dbName := os.Getenv("HMS_TEST_DATABASE")
	owner := os.Getenv("HADOOP_USER_NAME")
	if dbName == "" {
		dbName = "hms_test_database"
	}
	t.Log("Testing creating database", dbName, "and owner", owner)
	client, err := getClient(t)
	if err != nil {
		return
	}
	defer client.Close()
	description := "test database"
	err = client.CreateDatabase(&hmsclient.Database{Name: dbName, Description: description, Owner: owner})
	if err != nil {
		t.Error("failed to create database:", err)
		return
	}
	db, err := client.GetDatabase(dbName)
	if err != nil {
		t.Error("failed to get database:", err)
		return
	}
	if db.Name != dbName {
		t.Errorf("dbname %s is not equal %s", db.Name, dbName)
	}
	if description != db.Description {
		t.Errorf("description %s is not equal %s", db.Description, description)
	}
	if owner != db.Owner {
		t.Errorf("owner %s is not equal %s", db.Owner, owner)
	}
	err = client.DropDatabase(dbName, true, false)
	if err != nil {
		t.Error("failed to drop database", err)
		return
	}
}

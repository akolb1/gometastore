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

package main

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"

	"log"

	"github.com/akolb1/gometastore/hmsclient"
	"github.com/gorilla/mux"
	"strconv"
)

func getClient(w http.ResponseWriter, r *http.Request) (*hmsclient.MetastoreClient, error) {
	server := r.URL.Query().Get("NS")
	if server == "" {
		server = hmsHost
	}
	client, err := hmsclient.Open(server, hmsPort)
	if err != nil {
		w.Header().Set("X-HMS-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return nil, err
	}
	return client, err
}

func index(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, %q %s:%d", html.EscapeString(r.URL.Path), hmsHost, hmsPort)
}

func databaseList(w http.ResponseWriter, r *http.Request) {
	client, err := getClient(w, r)
	if err != nil {
		return
	}
	defer client.Close()
	databases, err := client.GetAllDatabases()
	if err != nil {
		w.Header().Set("X-HMS-Error", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.Header().Set("Content-Type", jsonEncoding)
	json.NewEncoder(w).Encode(databases)
}

func databaseShow(w http.ResponseWriter, r *http.Request) {
	client, err := getClient(w, r)
	if err != nil {
		return
	}
	defer client.Close()
	vars := mux.Vars(r)
	dbName := vars[paramDbName]
	database, err := client.GetDatabase(dbName)
	if err != nil {
		w.Header().Set("X-HMS-Error", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", jsonEncoding)
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(database)
}

func createDatabase(w http.ResponseWriter, r *http.Request) {
	client, err := getClient(w, r)
	if err != nil {
		return
	}
	defer client.Close()
	vars := mux.Vars(r)
	var db hmsclient.Database
	_ = json.NewDecoder(r.Body).Decode(&db)
	db.Name = vars[paramDbName]
	/*
		if db.Location == "" {
			db.Location = locationUri + db.Name + ".db"
		}
	*/
	if db.Owner == "" {
		db.Owner = r.URL.Query().Get("owner")
	}

	log.Println(fmt.Sprintf("Creating database %#v", db))
	err = client.CreateDatabase(db)
	if err != nil {
		w.Header().Set("X-HMS-Error", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}
	database, err := client.GetDatabase(db.Name)
	if err != nil {
		w.Header().Set("X-HMS-Error", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", jsonEncoding)
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(database)
}

func dropDatabase(w http.ResponseWriter, r *http.Request) {
	client, err := getClient(w, r)
	if err != nil {
		return
	}
	defer client.Close()
	vars := mux.Vars(r)
	dbName := vars[paramDbName]
	deleteData, _ := strconv.ParseBool(r.URL.Query().Get("data"))
	cascade, _ := strconv.ParseBool(r.URL.Query().Get("cascade"))
	log.Println(r.Host+r.URL.String(), r.Method)
	log.Println("Drop database", dbName, "d =", deleteData, "c =", cascade)
	err = client.DropDatabase(dbName, true, false)
	if err != nil {
		w.Header().Set("X-HMS-Error", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func listTables(w http.ResponseWriter, r *http.Request) {
	client, err := getClient(w, r)
	if err != nil {
		return
	}
	defer client.Close()
	vars := mux.Vars(r)
	dbName := vars[paramDbName]
	tables, err := client.GetAllTables(dbName)
	if err != nil {
		w.Header().Set("X-HMS-Error", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", jsonEncoding)
	json.NewEncoder(w).Encode(tables)
}

func tableShow(w http.ResponseWriter, r *http.Request) {
	client, err := getClient(w, r)
	if err != nil {
		return
	}
	defer client.Close()
	vars := mux.Vars(r)
	dbName := vars[paramDbName]
	tableName := vars[paramTblName]
	table, err := client.GetTable(dbName, tableName)
	if err != nil {
		w.Header().Set("X-HMS-Error", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", jsonEncoding)

	json.NewEncoder(w).Encode(table)
}

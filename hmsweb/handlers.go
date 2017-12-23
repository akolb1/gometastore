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
)

func index(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, %q %s:%d", html.EscapeString(r.URL.Path), hmsHost, hmsPort)
}

func databaseList(w http.ResponseWriter, r *http.Request) {
	client, err := hmsclient.Open(hmsHost, hmsPort)
	defer client.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
	databases, err := client.GetAllDatabases()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.Header().Set("Content-Type", jsonEncoding)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(databases)
}

func databaseShow(w http.ResponseWriter, r *http.Request) {
	client, err := hmsclient.Open(hmsHost, hmsPort)
	defer client.Close()
	if err != nil {
		fmt.Fprintf(w, "%v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	vars := mux.Vars(r)
	dbName := vars[paramDbName]
	database, err := client.GetDatabase(dbName)
	if err != nil {
		fmt.Fprintf(w, "%v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", jsonEncoding)
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(database)
}

func createDatabase(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	client, err := hmsclient.Open(hmsHost, hmsPort)
	defer client.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
	var db hmsclient.Database
	_ = json.NewDecoder(r.Body).Decode(&db)
	db.Name = vars[paramDbName]
	if db.Location == "" {
		db.Location = locationUri + db.Name + ".db"
	}
	log.Println(fmt.Sprintf("Creating database %#v", db))
	err = client.CreateDatabase(db)
	if err != nil {
		log.Println("error:", err)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%v", err)
		return
	}
	database, err := client.GetDatabase(db.Name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
	w.Header().Set("Content-Type", jsonEncoding)
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(database)
}

func dropDatabase(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	dbName := vars[paramDbName]
	log.Println("Drop database", dbName)
	client, err := hmsclient.Open(hmsHost, hmsPort)
	defer client.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
	err = client.DropDatabase(dbName, true, false)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%v", err)
		return
	}
	err = client.DropDatabase(dbName, true, false)
	w.WriteHeader(http.StatusOK)
}

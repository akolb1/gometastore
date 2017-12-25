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
	"flag"
	"log"
	"net/http"

	"fmt"
	"github.com/gorilla/mux"
)

const (
	hmsPortDefault = 9083

	jsonEncoding = "application/json; charset=UTF-8"

	paramDbName  = "dbName"
	paramTblName = "tableName"
)

var (
	hmsPort int
)

func main() {
	flag.IntVar(&hmsPort, "port", hmsPortDefault, "HMS Port")
	flag.Parse()

	router := mux.NewRouter()

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
			t, err := route.GetPathTemplate()
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "%s\n", t)
			return nil
		})
	})

	router.HandleFunc("/{host}/databases", databaseList)
	router.HandleFunc("/{host}", databaseList)
	router.HandleFunc("/{host}/databases/{dbName}", databaseShow).Methods("GET")
	router.HandleFunc("/{host}/{dbName}", databaseShow).Methods("GET")
	router.HandleFunc("/{host}/databases/{dbName}", databaseCreate).Methods("POST")
	router.HandleFunc("/{host}/databases/{dbName}", databaseDrop).Methods("DELETE")
	router.HandleFunc("/{host}/databases/{dbName}/", tablesList).Methods("GET")
	router.HandleFunc("/{host}/{dbName}/", tablesList).Methods("GET")
	router.HandleFunc("/{host}/databases/{dbName}/{tableName}", tablesShow).Methods("GET")
	router.HandleFunc("/{host}/{dbName}/{tableName}", tablesShow).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", router))
}

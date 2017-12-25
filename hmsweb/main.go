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

	"github.com/gorilla/mux"
)

const (
	hmsPortDefault     = 9083
	hmsHostDefault     = "localhost"
	hmsLocationDefault = "hdfs://localhost:8020/user/hive/warehouse/"

	jsonEncoding = "application/json; charset=UTF-8"

	paramDbName  = "dbName"
	paramTblName = "tableName"
)

var (
	hmsHost     string
	hmsPort     int
	locationUri string
)

func main() {
	flag.StringVar(&hmsHost, "host", hmsHostDefault, "HMS host")
	flag.StringVar(&locationUri, "location", hmsLocationDefault, "HDFS location")
	flag.IntVar(&hmsPort, "port", hmsPortDefault, "HMS Port")
	flag.Parse()

	router := mux.NewRouter()
	router.HandleFunc("/", index)
	router.HandleFunc("/databases", databaseList)
	router.HandleFunc("/databases/{dbName}", databaseShow).Methods("GET")
	router.HandleFunc("/databases/{dbName}", createDatabase).Methods("POST")
	router.HandleFunc("/databases/{dbName}", dropDatabase).Methods("DELETE")
	router.HandleFunc("/databases/{dbName}/", listTables).Methods("GET")
	router.HandleFunc("/databases/{dbName}/{tableName}", tableShow).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", router))
}

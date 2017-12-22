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
	hmsPortDefault  = 9083
	hmsHostDefaiult = "localhost"

	jsonEncoding = "application/json; charset=UTF-8"

	paramDbName = "dbName"
)

var (
	hmsHost string
	hmsPort int
)

func main() {
	flag.StringVar(&hmsHost, "host", hmsHostDefaiult, "HMS host")
	flag.IntVar(&hmsPort, "port", hmsPortDefault, "HMS Port")
	flag.Parse()

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", index)
	router.HandleFunc("/databases", databaseList)
	router.HandleFunc("/databases/{dbName}", databaseShow)
	log.Fatal(http.ListenAndServe(":8080", router))
}

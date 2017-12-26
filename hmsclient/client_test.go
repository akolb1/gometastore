package hmsclient_test

import (
	"fmt"
	"log"

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
	fmt.Println(client.GetAllDatabases())
}

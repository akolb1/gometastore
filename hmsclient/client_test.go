package hmsclient

import (
	"fmt"
	"log"
)

func ExampleOpen() {
	client, err := Open("localhost", 9083)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(client.GetAllDatabases())
}

func ExampleMetastoreClient_GetAllDatabases() {
	client, err := Open("localhost", 9083)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(client.GetAllDatabases())
}

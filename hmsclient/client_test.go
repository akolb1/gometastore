package hmsclient_test

import (
	"fmt"
	"log"
	"testing"

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

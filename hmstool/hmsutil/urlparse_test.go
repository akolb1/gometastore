package hmsutil

import (
	"fmt"
	"testing"
)

const (
	scheme   = "hdfs"
	hostPort = "host.domain.com:1234"
	location = "/a/b/c"
)

func TestGetHostLocation(t *testing.T) {
	uri := fmt.Sprintf("%s://%s%s", scheme, hostPort, location)
	host, loc, err := GetHostLocation(uri)
	if err != nil {
		t.Errorf("failed parsing: %v", err)
		t.Fail()
	}
	if host != hostPort {
		t.Errorf("invalid host and port: %s", host)
	}
	if loc != location {
		t.Errorf("location %s doesn't match: %s", location, loc)
	}
}

func ExampleGetHostLocation() {
	uri := fmt.Sprintf("%s://%s%s", scheme, hostPort, location)
	host, loc, _ := GetHostLocation(uri)
	fmt.Println(host)
	fmt.Println(loc)
	// Output:
	// host.domain.com:1234
	// /a/b/c
}

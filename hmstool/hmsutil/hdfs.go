package hmsutil

import (
	"github.com/colinmarc/hdfs"
)

var connections map[string]*hdfs.Client

// List files in the given location
func ListFiles(location string) ([]string, error) {
	hostPort, path, err := GetHostLocation(location)
	if err != nil {
		return nil, err
	}
	client, ok := connections[hostPort]
	if !ok {
		client, err = hdfs.New(hostPort)
		if err != nil {
			return nil, err
		}
		connections[hostPort] = client
	}
	files, err := client.ReadDir(path)
	if err != nil {
		return nil, err
	}
	result := []string{}
	for _, file := range files {
		// Skip directories
		if !file.IsDir() {
			result = append(result, file.Name())
		}
	}
	return result, nil
}

func init() {
	connections = make(map[string]*hdfs.Client)
}

package cmd

import (
	"github.com/akolb1/gometastore/client"
	"github.com/spf13/viper"
)

// getClient returns Sentry API client, extracting parameters like host and port
// from viper.
//
// If component is specified, it uses Generic sentry protocol, otherwise it uses legacy
// protocol
func getClient() (*client.MetastoreClient, error) {
	host := viper.GetString(hostOpt)
	port := viper.GetInt(portOpt)
	return client.Open(host, port)
}

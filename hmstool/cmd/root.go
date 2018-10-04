// Copyright © 2018 Alex Kolbasov
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

package cmd

import (
	"fmt"
	"os"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	defaultThriftPort = "9083"
	hostOpt           = "host"
	portOpt           = "port"
	ownerOpt          = "owner"
	outputOpt         = "output"

	hadoopUserEnv = "HADOOP_USER_NAME"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hmstool",
	Short: "Hive metastore hmsclient tool",
	Run:   listTables,
	Long: `Command line hive metastore hmsclient tool

The tool works with HMS over its thrift API. The metastore host can be specified
using either -H command-line argument or HMS_HOST environment variable.

Examples:

All examples assume that HMS_HOST point to correct metastore. For example:

	export HMS_HOST=mymetastore.mycompany.com

1. List all tables
	$ hmstool
	default.tbl1
	default.mydb
	customers.data

2. List all databases

	$ hmstool db list
	default
	mydb

3. List all tables in a database default

	$ hmstool table list -d default
	default.tbl1
	default.mydb
`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getOwner() string {
	owner := viper.GetString(ownerOpt)
	if owner == "" {
		owner = os.Getenv(hadoopUserEnv)
	}
	return owner
}

func init() {
	cobra.OnInitialize(initConfig)
	hadoopUser := os.Getenv(hadoopUserEnv)
	if hadoopUser == "" {
		hadoopUser = "hive"
	}

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.hmstool.yaml)")
	rootCmd.PersistentFlags().StringP(hostOpt, "H", "localhost", "hostname for HMS server")
	rootCmd.PersistentFlags().StringP(portOpt, "p", defaultThriftPort, "port for HMS server")
	rootCmd.PersistentFlags().StringP(ownerOpt, "U", hadoopUser, "owner name")
	rootCmd.PersistentFlags().StringP(outputOpt, "o", "", "output file")

	// Bind flags to viper variables
	viper.BindPFlags(rootCmd.PersistentFlags())
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".hmstool" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".hmstool")
	}

	viper.SetConfigName(".hmstool") // name of config file (without extension)
	viper.AddConfigPath("$HOME")    // adding home directory as first search path
	viper.SetEnvPrefix("hms")       // All environment vars should start with SENTRY_
	viper.AutomaticEnv()            // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

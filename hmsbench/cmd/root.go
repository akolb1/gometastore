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

// TODO read hadoop config

package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/akolb1/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

const (
	hadoopUserName = "HADOOP_USER_NAME"
	stringType     = "string" // HMS representation of string type

	defaultThriftPort = "9083"
	hostOpt           = "host"
	portOpt           = "port"

	iterOpt     = "benchmark"
	outputOpt   = "output"
	warmOpt     = "warmup"
	sanitizeOpt = "sanitize"
	csvOpt      = "csv"
	ownerOpt    = "user"
	dbOpt       = "database"
	saveOpt     = "savedata"
	objectsOpt  = "objects"
	listOpt     = "list"
	filterOpt   = "filter"
	threadOpt   = "threads"

	scale = 1000000
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hmsbench",
	Short: "HMS Benchmarks",
	Long:  `HMS Benchmarks`,
	Run:   run,
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
		owner = os.Getenv(hadoopUserName)
	}
	return owner
}

// getSchema converts argument to list of field schemas.
// Schema is represented as name=type,.... If type is missing, "string" is assumed.
func getSchema(arg string) []hive_metastore.FieldSchema {
	// First split on commas
	if arg == "" {
		return nil
	}
	fields := strings.Split(arg, ",")
	if len(fields) == 0 {
		return nil
	}
	schema := make([]hive_metastore.FieldSchema, 0, len(fields))
	for _, s := range fields {
		name := s
		typ := stringType
		parts := strings.Split(s, "=")
		if len(parts) == 2 {
			name = parts[0]
			typ = parts[1]
		}
		schema = append(schema, hive_metastore.FieldSchema{Name: name, Type: typ})
	}
	return schema
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.hmsbench.yaml)")
	rootCmd.PersistentFlags().StringP(hostOpt, "H", "localhost", "hostname for HMS server")
	rootCmd.PersistentFlags().StringP(portOpt, "P", defaultThriftPort, "port for HMS server")
	rootCmd.Flags().IntP(iterOpt, "B", 100, "number of benchmark iterations")
	rootCmd.Flags().IntP(warmOpt, "W", 15, "number of warmup iterations")
	rootCmd.Flags().BoolP(sanitizeOpt, "S", false, "sanitize results")
	rootCmd.Flags().BoolP(csvOpt, "C", false, "output in CSV format")
	rootCmd.Flags().BoolP(listOpt, "L", false, "list benchmarks instead of runnig them")
	rootCmd.PersistentFlags().StringP(ownerOpt, "u", "user", "owner name")
	rootCmd.PersistentFlags().StringP(dbOpt, "d", "", "owner name")
	rootCmd.PersistentFlags().StringP(outputOpt, "o", "", "output file")
	rootCmd.PersistentFlags().StringP(saveOpt, "", "", "location for raw benchmark data")
	rootCmd.PersistentFlags().StringP(filterOpt, "F", "", "run benchmarks matching the filter")
	rootCmd.PersistentFlags().IntP(objectsOpt, "N", 100, "number of objects to create")
	rootCmd.PersistentFlags().IntP(threadOpt, "T", 1, "number concurrent threads")
	// Bind flags to viper variables
	viper.BindPFlags(rootCmd.PersistentFlags())
	viper.BindPFlags(rootCmd.Flags())
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

		// Search config in home directory with name ".hmsbench" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".hmsbench")
	}

	viper.SetEnvPrefix("hms") // All environment vars should start with SENTRY_
	viper.AutomaticEnv()      // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

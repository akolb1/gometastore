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

	hadoopUserEnv = "HADOOP_USER_NAME"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hmstool",
	Short: "Hive metastore hmsclient tool",
	Long:  `Command line hive metastore hmsclient tool`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) {},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
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

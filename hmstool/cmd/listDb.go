package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

func listDbs(cmd *cobra.Command, args []string) {
	client, err := getClient()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	databases, err1 := client.GetAllDatabases()
	if err1 != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, r := range databases {
		fmt.Println(r)
	}
}

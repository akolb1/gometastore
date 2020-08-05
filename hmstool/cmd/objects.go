package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/terry-sm/gometastore/hmsclient"
	"github.com/terry-sm/gometastore/hmsclient/thrift/gen-go/hive_metastore"
	"github.com/spf13/viper"
)

type HmsObject struct {
	Databases  []*hmsclient.Database       `json:"databases,omitempty"`
	Tables     []*hive_metastore.Table     `json:"tables,omitempty"`
	Partitions []*hive_metastore.Partition `json:"partitions,omitempty"`
}

func displayObject(hmsObject *HmsObject) {
	outputFileName := viper.GetString(outputOpt)
	if outputFileName == "" {
		b, _ := json.MarshalIndent(hmsObject, "", "  ")
		fmt.Println(string(b))
	} else {
		b, _ := json.Marshal(hmsObject)
		if err := ioutil.WriteFile(outputFileName, b, 0644); err != nil {
			log.Println("failed to write data to file", outputFileName, err)
		}
	}

}

package cmd

import (
	"fmt"
	"nats_vs_kafka/utils"
	"strings"

	"github.com/spf13/viper"
)

var (
	defaultConfigFile string = "config.yaml"
	configFile        string
	configName        string
	configType        string
)

func parsingConfig() {
	configSlice := strings.Split(configFile, ".")
	if len(configSlice) != 2 {
		utils.Logger.Errorln("Wrong configName set, using default value!")
		configFile = defaultConfigFile
		configSlice = strings.Split(configFile, ".")
	}
	configName = configSlice[0]
	configType = configSlice[1]
}

// read config.yaml
func readConfig() error {
	// 1. read from config file
	viper.SetConfigName(configName) // name of config file (without extension)
	viper.SetConfigType(configType) // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")        // optionally look for config in the working directory
	err := viper.ReadInConfig()     // Find and read the config file
	if err != nil {                 // Handle errors reading the config file
		utils.Logger.Errorf("Fatal error config file: %s \n", err)
		return err
	}

	// 2. check specific config
	// TODO: check config (not finished)
	checkConfigMap("topics", []string{"nats", "kafka"})
	checkConfigMap("addrs", []string{"nats", "kafka"})

	utils.Topics = viper.GetStringMapString("topics")
	utils.Addrs = viper.GetStringMapString("addrs")

	utils.Logger.Infoln("Configs read are:::", utils.Topics, utils.Addrs)

	return nil
}

func checkStringMap(mapRaw map[string]interface{}, itemName string, subkeys []string) {
	for _, v := range subkeys {
		if _, ok := mapRaw[v]; !ok {
			err := fmt.Errorf("Config item[%s] not contains key '%s'", itemName, v)
			utils.Logger.Errorln(err)
			panic(err)
		}
	}
}

// panic if the config file is wrong
func checkConfigMap(key string, subkeys []string) {
	if !viper.IsSet(key) {
		err := fmt.Errorf("Config file not contains key '%s'", key)
		utils.Logger.Errorln(err)
		panic(err)
	} else {
		mapRoot := viper.GetStringMap(key)
		for _, subKey := range subkeys {
			if _, ok := mapRoot[subKey]; !ok {
				err := fmt.Errorf("Config file not contains key '%s/%s'", key, subKey)
				utils.Logger.Errorln(err)
				panic(err)
			}
		}
	}
}

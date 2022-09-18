package util

import (
	_ "github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type Config struct {
	KafkaServer  string `mapstructure:"KAFKA_SERVER"`
	KafkaTopic   string `mapstructure:"KAFKA_TOPIC"`
	KafkaGroupId string `mapstructure:"KAFKA_GROUPID"`
}

func LoadConfig(path string) (config Config, err error) {
	// viper.AddConfigPath(path)
	// viper.SetConfigName("app")
	// viper.SetConfigType("env")
	viper.SetConfigFile(path)
	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}

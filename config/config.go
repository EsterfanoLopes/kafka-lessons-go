package config

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

var Cfg *Config
var Logger *log.Logger

type Config struct {
	Addresses    []string
	SaramaConfig *sarama.Config
}

/*
KafkaInit initiates configuration needed to run producers and consumers
*/
func KafkaInit() {
	if Cfg != nil {
		return
	}

	c := sarama.NewConfig()
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	c.Producer.Flush.Messages = 1

	cfg := Config{
		SaramaConfig: c,
		Addresses: []string{
			"127.0.0.1:29092",
		},
	}

	Cfg = &cfg

	err := Cfg.SaramaConfig.Validate()
	if err != nil {
		panic(fmt.Sprintf("error on kafka setup %s", err))
	}
}

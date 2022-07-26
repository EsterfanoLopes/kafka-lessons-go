package config

import (
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

var Cfg *Config
var Logger *log.Logger

type Config struct {
	Addresses    []string
	SaramaConfig *sarama.Config
	Logger       *log.Logger
}

func Init() {
	kafkaInit()
	loggerInit()
}

/*
KafkaInit initiates configuration needed to run producers and consumers
*/
func kafkaInit() {
	if Cfg != nil {
		return
	}

	c := sarama.NewConfig()
	// Producer
	c.Producer.Return.Successes = true
	c.Producer.Return.Errors = true
	c.Producer.Flush.Messages = 1

	// Consumer
	c.Consumer.Return.Errors = true

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

/*
loggerInit initiates default info logger
*/
func loggerInit() {
	if Cfg.Logger != nil {
		return
	}

	Cfg.Logger = log.New(os.Stdout, "INFO\t", log.Ldate|log.Ltime)
}

package config

import "github.com/Shopify/sarama"

var Cfg *Config

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
	cfg := Config{
		SaramaConfig: c,
		Addresses: []string{
			"127.0.0.1:29092",
		},
	}

	Cfg = &cfg
}

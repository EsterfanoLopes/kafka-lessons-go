package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"strings"

	"github.com/EsterfanoLopes/kafka-lessons-go/config"
	"github.com/EsterfanoLopes/kafka-lessons-go/data"
	"github.com/Shopify/sarama"
)

type Consumer struct {
	Client           sarama.Consumer
	ErrorChan        chan *sarama.ConsumerError
	MessageChan      chan *sarama.ConsumerMessage
	DoneChan         chan struct{}
	Signals          chan os.Signal
	NumberOfMessages int
}

func InitConsumerConfig() Consumer {
	client, err := sarama.NewConsumer(config.Cfg.Addresses, config.Cfg.SaramaConfig)
	if err != nil {
		config.Cfg.Logger.Println(err)
	}

	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)
	done := make(chan struct{})
	signals := make(chan os.Signal, 1)

	return Consumer{
		Client:      client,
		MessageChan: consumers,
		ErrorChan:   errors,
		DoneChan:    done,
		Signals:     signals,
	}
}

func main() {
	config.Init()
	// Create new consumer
	c := InitConsumerConfig()
	defer func() {
		if err := c.Client.Close(); err != nil {
			config.Cfg.Logger.Println(err)
		}

		close(c.DoneChan)
		close(c.ErrorChan)
		close(c.Signals)
	}()

	// Get client topics
	topics, err := c.Client.Topics()
	if err != nil {
		config.Cfg.Logger.Println(err)
	}

	consumeTopics(c, topics)
	signal.Notify(c.Signals, os.Interrupt)

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-c.MessageChan:
				c.NumberOfMessages++
				config.Cfg.Logger.Println("Received messages", string(msg.Key), string(msg.Value))
			case consumerError := <-c.ErrorChan:
				c.NumberOfMessages++
				config.Cfg.Logger.Println("Received consumerError ", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
				doneCh <- struct{}{}
			case <-c.Signals:
				config.Cfg.Logger.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	config.Cfg.Logger.Println("Processed", c.NumberOfMessages, "messages")

}

func consumeTopics(c Consumer, topics []string) {
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := c.Client.Partitions(topic)

		for _, partition := range partitions {
			partitionConsumer, err := c.Client.ConsumePartition(topic, partition, sarama.OffsetOldest)
			if nil != err {
				config.Cfg.Logger.Printf("Topic %v Partition: %v", topic, partition)
				config.Cfg.Logger.Println(err)
			}
			config.Cfg.Logger.Println(" Start consuming topic ", topic)
			go consumePartition(c, topic, partitionConsumer)
		}
	}
}

func consumePartition(c Consumer, topic string, consumer sarama.PartitionConsumer) {
	for {
		select {
		case consumerError := <-consumer.Errors():
			c.ErrorChan <- consumerError
			config.Cfg.Logger.Println("consumerError: ", consumerError.Err)

		case msg := <-consumer.Messages():
			c.MessageChan <- msg

			r := data.Record{}
			json.Unmarshal(msg.Value, &r)

			config.Cfg.Logger.Println("Got message on topic ", topic, r)
		}
	}
}

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/EsterfanoLopes/kafka-lessons-go/config"
	"github.com/EsterfanoLopes/kafka-lessons-go/data"
	"github.com/Shopify/sarama"
)

func main() {
	config.Init()
	// Create new consumer
	master, err := sarama.NewConsumer(config.Cfg.Addresses, config.Cfg.SaramaConfig)
	if err != nil {
		config.Cfg.Logger.Println(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			config.Cfg.Logger.Println(err)
		}
	}()

	topics, _ := master.Topics()

	consumer, errors := consume(topics, master)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-consumer:
				msgCount++
				config.Cfg.Logger.Println("Received messages", string(msg.Key), string(msg.Value))
			case consumerError := <-errors:
				msgCount++
				config.Cfg.Logger.Println("Received consumerError ", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
				doneCh <- struct{}{}
			case <-signals:
				config.Cfg.Logger.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	config.Cfg.Logger.Println("Processed", msgCount, "messages")

}

func consume(topics []string, master sarama.Consumer) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := master.Partitions(topic)
		// this only consumes partition no 1, you would probably want to consume all partitions
		consumer, err := master.ConsumePartition(topic, partitions[0], sarama.OffsetOldest)
		if nil != err {
			fmt.Printf("Topic %v Partitions: %v", topic, partitions)
			config.Cfg.Logger.Println(err)
		}
		config.Cfg.Logger.Println(" Start consuming topic ", topic)
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					errors <- consumerError
					config.Cfg.Logger.Println("consumerError: ", consumerError.Err)

				case msg := <-consumer.Messages():
					consumers <- msg

					r := data.Record{}

					json.Unmarshal(msg.Value, &r)

					config.Cfg.Logger.Println("Got message on topic ", topic, r)
				}
			}
		}(topic, consumer)
	}

	return consumers, errors
}

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/EsterfanoLopes/kafka-lessons-go/config"
	"github.com/EsterfanoLopes/kafka-lessons-go/data"

	"github.com/Shopify/sarama"
)

type Producer struct {
	AsyncClient         sarama.AsyncProducer
	ErrorChan           chan error
	ProductionWaitGroup sync.WaitGroup
	NumberOfMessages    int
}

const (
	numberOfMessages = 10
)

/*
 InitProducerConfig

 Initiates producer config with demanded channels, waitgroups and async producer.
*/
func InitProducerConfig() Producer {
	var (
		productionWaitGroup sync.WaitGroup

		errorChan = make(chan error)
	)

	asyncProducer, err := sarama.NewAsyncProducer(config.Cfg.Addresses, config.Cfg.SaramaConfig)
	if err != nil {
		panic(err)
	}

	return Producer{
		AsyncClient:         asyncProducer,
		ErrorChan:           errorChan,
		ProductionWaitGroup: productionWaitGroup,
		NumberOfMessages:    numberOfMessages,
	}
}

func main() {
	config.KafkaInit()
	producerConfig := InitProducerConfig()

	/* Routines */
	// To listen for errors
	go listenForErrors(producerConfig.AsyncClient, &producerConfig.ProductionWaitGroup, producerConfig.ErrorChan)
	// To listen for successes
	go listenForSuccess(producerConfig.AsyncClient, &producerConfig.ProductionWaitGroup)
	// To Produce
	for i := 0; i < producerConfig.NumberOfMessages; i++ {
		go produce(producerConfig.AsyncClient, &producerConfig.ProductionWaitGroup, producerConfig.ErrorChan)
	}

	// Shutdown
	producerConfig.ProductionWaitGroup.Wait()
	go stop(producerConfig)
}

func stop(p Producer) {
	p.AsyncClient.AsyncClose()
	close(InitProducerConfig().ErrorChan)

	fmt.Printf("Done without errors")
	os.Exit(0)
}

// listenForErrors waits on error channels, prints it if received.
func listenForErrors(asyncProducer sarama.AsyncProducer, pwg *sync.WaitGroup, producerErrorChan chan error) {
	select {
	case err := <-asyncProducer.Errors():
		fmt.Println(err)
		pwg.Done()
	case err := <-producerErrorChan:
		fmt.Println(err)
	}
}

// listenForSuccess prints success sent messaes
func listenForSuccess(asyncProducer sarama.AsyncProducer, pwg *sync.WaitGroup) {
	msg := <-asyncProducer.Successes()
	fmt.Println(msg)
	pwg.Done()
}

func produce(producer sarama.AsyncProducer, pwg *sync.WaitGroup, errorChan chan error) {
	// generate any data
	r := data.Record{
		Value: "teste",
	}

	// marshals into bytes
	br, err := json.Marshal(r)
	if err != nil {
		errorChan <- err
	}

	// build message
	msg := sarama.ProducerMessage{
		Topic:     "test_go",
		Key:       sarama.StringEncoder("test"),
		Value:     sarama.StringEncoder(br),
		Headers:   []sarama.RecordHeader{},
		Metadata:  nil,
		Timestamp: time.Now(),
	}

	// send message to channel to be sent to kafka broker
	producer.Input() <- &msg

	// increment wait group who controls producer messages
	pwg.Add(1)
}

package main

import (
	"encoding/json"
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
	ProductionWaitGroup *sync.WaitGroup
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

		errorChan = make(chan error, 1)
	)

	asyncProducer, err := sarama.NewAsyncProducer(config.Cfg.Addresses, config.Cfg.SaramaConfig)
	if err != nil {
		panic(err)
	}

	return Producer{
		AsyncClient:         asyncProducer,
		ErrorChan:           errorChan,
		ProductionWaitGroup: &productionWaitGroup,
		NumberOfMessages:    numberOfMessages,
	}
}

func main() {
	config.Init()

	p := InitProducerConfig()

	/* Routines */
	// To listen for errors
	go listenForErrors(p)
	// To listen for successes
	go listenForSuccess(p)
	// To Produce
	for i := 0; i < p.NumberOfMessages; i++ {
		// increment wait group who controls producer messages
		p.ProductionWaitGroup.Add(1)
		config.Cfg.Logger.Println("Sending message ", i)
		go produce(p)
	}

	// wait all messages be processed
	p.ProductionWaitGroup.Wait()

	// Shutdown
	go shutdown(p)
}

func shutdown(p Producer) {
	p.AsyncClient.AsyncClose()
	close(InitProducerConfig().ErrorChan)

	config.Cfg.Logger.Println("Finishing")
	os.Exit(0)
}

// listenForErrors waits on error channels, prints it if received.
func listenForErrors(p Producer) {
	go func() {
		for range p.AsyncClient.Errors() {
			p.ProductionWaitGroup.Done()
		}
	}()

	err := <-p.ErrorChan
	config.Cfg.Logger.Printf("error on producer logic: %v\n", err)
}

// listenForSuccess inform waitgroup to increase
func listenForSuccess(p Producer) {
	for range p.AsyncClient.Successes() {
		p.ProductionWaitGroup.Done()
	}
}

func produce(p Producer) {
	// generate any data
	r := data.Record{
		Value: "teste",
	}

	// marshals into bytes
	br, err := json.Marshal(r)
	if err != nil {
		p.ErrorChan <- err
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
	p.AsyncClient.Input() <- &msg
}

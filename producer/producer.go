package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

const (
	KafkaServerAddress = "localhost:9092"
	KafkaTopicEven     = "even"
	KafkaTopicOdd      = "odd"
)

func sendKafkaMessage(producer sarama.SyncProducer, val int, topic string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("number"),
		Value: sarama.StringEncoder(strconv.Itoa(val)),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Partition for %d is %d \n ", val, partition)
	log.Printf("Offset for %d is %d \n ", val, offset)

	return nil
}

func sendMessageFunction(producer sarama.SyncProducer) {
	log.Println("Producer is started successfully")
	i := 1
	for {

		var topic string

		if i%2 == 0 {
			topic = KafkaTopicEven

		} else {
			topic = KafkaTopicOdd
		}

		err := sendKafkaMessage(producer, i, topic)
		if err != nil {
			log.Println("producer is closed successfully")
			return
		}
		i++

		time.Sleep(1 * time.Second)
	}
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress},
		config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

func main() {
	producer, err := setupProducer()
	if err != nil {
		log.Println("error in setting up the server ", err)
		return
	}

	defer producer.Close()

	sendMessageFunction(producer)
}

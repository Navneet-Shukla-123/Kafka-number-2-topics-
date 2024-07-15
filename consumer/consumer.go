package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	ConsumerGroup      = "number-group"
	ConsumerTopicEven  = "even"
	ConsumerTopicOdd   = "odd"
	KafkaServerAddress = "localhost:9092"
)

type Number map[string][]int

type NumberStore struct {
	data Number
	mu   sync.Mutex
}

func (ns *NumberStore) Add(value int) {
	ns.mu.Lock()
	if value%2 == 0 {
		ns.data["even"] = append(ns.data["even"], value)
	} else {
		ns.data["odd"] = append(ns.data["odd"], value)

	}

	ns.mu.Unlock()

}

func (ns *NumberStore) GetEven() []int {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return ns.data["even"]
}

func (ns *NumberStore) GetOdd() []int {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	return ns.data["odd"]
}

type Consumer struct {
	store *NumberStore
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer *Consumer) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		topic := msg.Topic

		key := string(msg.Key)
		log.Println("Key is ", key)
		value, err := strconv.Atoi(string(msg.Value))
		if err != nil {
			log.Println("error convertint he value to int ", err)
			continue
		}

		log.Println("value is ", value)
		log.Printf("This value %d is from topic %s \n",value,topic)

		consumer.store.Add(value)
		sess.MarkMessage(msg, "")
	}
	return nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return consumerGroup, nil
}

func setupConsumerGroup(ctx context.Context, store *NumberStore) {
	consumerGroup, err := initializeConsumerGroup()
	if err != nil {
		log.Printf("initialization error for consumer group: %v", err)
	}

	defer consumerGroup.Close()

	consumer := &Consumer{store: store}
	topics := []string{
		ConsumerTopicEven, ConsumerTopicOdd,
	}

	for {
		err = consumerGroup.Consume(ctx, topics, consumer)
		if err != nil {
			log.Printf("error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func getData(store *NumberStore) {
	for {
		dataEven := store.GetEven()
		dataOdd := store.GetOdd()

		log.Println("Even number is ")
		log.Println(dataEven)

		log.Println("Odd number is ")
		log.Println(dataOdd)

		time.Sleep(20 * time.Second)
	}
}

func main() {
	store := &NumberStore{
		data: make(Number),
	}
	ctx, cancel := context.WithCancel(context.Background())
	go setupConsumerGroup(ctx, store)
	defer cancel()

	getData(store)

}

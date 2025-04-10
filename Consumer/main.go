package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "coffee_orders"
	msgCnt := 0
	// 1. Create a consumer and start it
	worker, err := ConnectConsumer([]string{"localhost:9092"})
	if err != nil {
		log.Panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Panic(err)
	}

	fmt.Println("Consumer started")

	// 2. handle OS signal used to stop the process
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 3. Create a go routine to handle the consumer / Worker
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case msg := <-consumer.Messages():
				msgCnt++
				fmt.Printf("Recieved Order Count %d: | Topic(%s) | Message(%s) \n", msgCnt, msg.Topic, string(msg.Value))
				fmt.Printf("Message is %s\n", string(msg.Value))
				order := string(msg.Value)
				fmt.Printf("Brewing coffee for order: %s\n", order)
			case err := <-consumer.Errors():
				log.Println(err)
			case <-sigchan:
				fmt.Println("Interrupt if detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCnt, "messages")

	// 4. Close the consumer on exit
	if err := worker.Close(); err != nil {
		log.Panic(err)
	}
}

func ConnectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	return sarama.NewConsumer(brokers, config)
}

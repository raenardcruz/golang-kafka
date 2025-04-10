package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type Order struct {
	CustomName string `json:"custom_name"`
	CofeeType  string `json:"cofee_type"`
}

func main() {
	http.HandleFunc("/order", OrderHandler)
	log.Fatal(http.ListenAndServe(":3000", nil))
}

func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, config)
}

func PushOrdertoQueue(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}
	// Create a connection
	producer, err := ConnectProducer(brokers)
	if err != nil {
		return err
	}

	defer producer.Close()

	// Create a new message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("Order is stored inTopic(%s)/Partition(%d)/Offset(%d)\n", topic, partition, offset)

	return nil
}

func OrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// 1. Parse the request Body
	order := new(Order)
	err := json.NewDecoder(r.Body).Decode(order)
	if err != nil {
		log.Println("Error decoding JSON:", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	// 2. Convert Body into bytes
	orderInBytes, err := json.Marshal(order)
	if err != nil {
		log.Println("Error converting to JSON:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	// 3. Send the bytes to kafka
	err = PushOrdertoQueue("coffee_orders", orderInBytes)
	if err != nil {
		log.Println("Error pushing to Kafka:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// 4. Send a response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]string{"status": "success", "message": fmt.Sprintf("Order for %s with type %s is received", order.CustomName, order.CofeeType)}
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		log.Println("Error encoding JSON response:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// 5. Close the connection
}

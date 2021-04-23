package main

import (
	"context"
	"encoding/binary"
	"log"
	"time"

	dapr "github.com/dapr/go-sdk/client"
)

const keyName = "CounterKey"

var (
	storeName  = "statestore"
	pubsubName = "pubsub"
	topicName  = "counter"
)

func main() {
	c, err := dapr.NewClient()
	if err != nil {
		log.Fatal("Client creation failed")
	}
	defer c.Close()

	ctx := context.Background()

	for {
		time.Sleep(5 * time.Second)

		// Read from state
		item, err := c.GetState(ctx, storeName, keyName)
		if err != nil {
			log.Print("Reading state error", err)
			continue
		}
		var counter = uint64(0)
		if len(item.Value) > 0 {
			counter = binary.BigEndian.Uint64(item.Value)
		}

		// Process - Increase counter
		newCounter := counter + 1
		log.Printf("Counter: %v => %v", counter, newCounter)

		// Save back to state
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, newCounter)
		if err := c.SaveState(ctx, storeName, keyName, buf); err != nil {
			log.Print("Failed to set state", err)
			continue
		}

		// Publish to topic
		log.Printf("Publishing event.")
		if err := c.PublishEventfromCustomContent(ctx, pubsubName, topicName, newCounter); err != nil {
			log.Printf("Failed to publish event %v", counter)
			continue
		}
	}
}

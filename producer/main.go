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
	counter    = uint64(0)
	pubsubName = "pubsub"
	topicName  = "counter"
)

func main() {
	c, err := dapr.NewClient()
	if err != nil {
		log.Fatal("Client creation failed")
	}

	ctx := context.Background()

	for {
		// Read from state
		item, err := c.GetState(ctx, storeName, keyName)
		if err != nil {
			log.Panic("Reading state error")
		}
		if len(item.Value) == 0 {
			counter = 0
		} else {
			counter = binary.BigEndian.Uint64(item.Value)
		}
		log.Printf("Counter: %v => %v", counter, counter+1)

		// Process - Increase counter
		counter++
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, counter)

		// Save back to state
		err = c.SaveState(ctx, storeName, keyName, buf)
		if err != nil {
			log.Fatal("Failed to set state", err)
		}

		// Publish to topic
		if err = c.PublishEvent(ctx, pubsubName, topicName, buf); err != nil {
			log.Fatalf("Failed to publish event %v", counter)
		}

		time.Sleep(5 * time.Second)
	}
}

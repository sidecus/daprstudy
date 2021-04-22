package main

import (
	"context"
	"encoding/binary"
	"log"

	"github.com/dapr/go-sdk/service/common"
	"github.com/pkg/errors"

	//daprd "github.com/dapr/go-sdk/service/grpc"
	// Comment out below line and uncomment above line to use grpc
	daprd "github.com/dapr/go-sdk/service/http"
)

var (
	serviceAddress = ":27015"
	pubsubName     = "pubsub"
	topicName      = "counter"
	routeName      = "processcounter"
)

func main() {
	eventHandler := func(ctx context.Context, in *common.TopicEvent) (retry bool, err error) {
		log.Printf("Received event %s from source %s. DateType: %s", in.ID, in.Source, in.DataContentType)

		if in.DataContentType != "text/plain" {
			return false, errors.Errorf("Invalid content type %s (eventID: %s, source: %s)", in.DataContentType, in.ID, in.Source)
		}

		topic := in.Topic
		data := []byte(in.Data.(string))
		if len(data) != 8 {
			return false, errors.Errorf("Invalid payload data (eventID: %s, source: %s)", in.ID, in.Source)
		}

		// Process event
		value := binary.BigEndian.Uint64(data)
		log.Printf("Event processed for topic %s: %v", topic, value)

		return
	}

	var err error = nil
	// s, err := daprd.NewService(serviceAddress)
	// Comment out below line and uncomment above line to use grpc
	s := daprd.NewService(serviceAddress)
	if err != nil {
		log.Fatal("Service creation failed")
	}

	log.Print("Starting service")
	sub := &common.Subscription{
		PubsubName: pubsubName,
		Topic:      topicName,
		Route:      routeName,
	}
	if err := s.AddTopicEventHandler(sub, eventHandler); err != nil {
		log.Fatalf("error adding topic subscription: %v", err)
	}
	if err := s.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

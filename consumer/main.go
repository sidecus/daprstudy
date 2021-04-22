package main

import (
	"context"
	"encoding/binary"
	"log"
	"os"

	"github.com/dapr/go-sdk/service/common"
	"github.com/pkg/errors"

	daprd "github.com/dapr/go-sdk/service/grpc"
	daprdhttp "github.com/dapr/go-sdk/service/http"
)

var (
	serviceAddress = ":27015"
	pubsubName     = "pubsub"
	topicName      = "counter"
	routeName      = "processcounter"
)

func main() {
	mode := parseArgs()

	// Create service
	var s common.Service
	var err error = nil
	if mode == "http" {
		s = daprdhttp.NewService(serviceAddress)
	} else {
		s, err = daprd.NewService(serviceAddress)
	}

	if err != nil {
		log.Fatal("Service creation failed")
	}

	// Subscribe to counter topic
	log.Print("Starting service")
	sub := &common.Subscription{
		PubsubName: pubsubName,
		Topic:      topicName,
		Route:      routeName,
	}
	if err := s.AddTopicEventHandler(sub, topicEventHandler); err != nil {
		log.Fatalf("error adding topic subscription: %v", err)
	}

	// Add health service invocation handler
	if err := s.AddServiceInvocationHandler("health", healthHandler); err != nil {
		log.Fatalf("error adding topic subscription: %v", err)
	}

	// Start service
	if err := s.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func parseArgs() string {
	if len(os.Args) < 2 {
		return "http"
	}

	mode := os.Args[1]
	if mode != "http" && mode != "gRPC" {
		log.Fatalf("Invalid mode: %s", mode)
	}

	return mode
}

// Pubsub Event handler
func topicEventHandler(ctx context.Context, in *common.TopicEvent) (retry bool, err error) {
	log.Printf("Received event %s from %s. DateType: %s.", in.ID, in.Source, in.DataContentType)

	if in.DataContentType != "text/plain" {
		log.Print("Invalid content type received")
		return false, errors.Errorf("Invalid content type %s (eventID: %s, source: %s)", in.DataContentType, in.ID, in.Source)
	}

	topic := in.Topic
	data := []byte(in.Data.(string))
	log.Printf("Event payload: %v", data)
	if len(data) != 8 {
		log.Printf("Invalid data length, %v", data)
		return false, errors.Errorf("Invalid payload data (eventID: %s, source: %s)", in.ID, in.Source)
	}

	// Process event
	value := binary.BigEndian.Uint64(data)
	log.Printf("Event processed for topic %s: %v", topic, value)

	return
}

func healthHandler(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {
	log.Printf("Helth check: ContentType:%s, Verb:%s, QueryString:%s", in.ContentType, in.Verb, in.QueryString)
	// do something with the invocation here
	out = &common.Content{
		Data:        []byte("Healthy:" + in.QueryString),
		ContentType: "text/plain",
	}
	return
}

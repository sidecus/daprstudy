package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/dapr/go-sdk/service/common"
	"github.com/pkg/errors"

	daprd "github.com/dapr/go-sdk/service/grpc"
	daprdhttp "github.com/dapr/go-sdk/service/http"
)

var (
	pubsubName = "pubsub"
	topicName  = "counter"
	routeName  = "processcounter"
)

func main() {
	mode, port := parseArgs()

	// Create service
	var serviceAddress = fmt.Sprintf(":%v", port)
	var s common.Service
	var err error = nil
	if mode == "http" {
		s = daprdhttp.NewService(serviceAddress)
	} else {
		s, err = daprd.NewService(serviceAddress)
	}

	if err != nil {
		log.Fatal("Service creation failed", err)
	}

	// Subscribe to counter topic
	log.Printf("Starting service..., mode: %s, port: %v", mode, port)
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

func parseArgs() (mode string, port int) {
	flag.StringVar(&mode, "consumermode", "http", "http/gRPC")
	flag.IntVar(&port, "consumerport", 27015, "app port")
	flag.Parse()

	if mode != "http" && mode != "gRPC" {
		log.Fatalf("Invalid mode: %s", mode)
	}

	if port <= 0 {
		log.Fatalf("Invalid prot: %v", port)
	}

	return
}

// Pubsub Event handler
func topicEventHandler(ctx context.Context, in *common.TopicEvent) (retry bool, err error) {
	log.Printf("Received event %s from %s. DateType: %s.", in.ID, in.Source, in.DataContentType)

	// Current goSDK doesn't set data content type to application/json even though the payload is JSON
	if in.DataContentType != "text/plain" && in.DataContentType != "application/json" {
		log.Print("Invalid content type received")
		return false, errors.Errorf("Invalid content type %s (eventID: %s, source: %s)", in.DataContentType, in.ID, in.Source)
	}

	topic := in.Topic
	data := in.Data.(string)
	log.Printf("Event payload: %v", data)

	// Deserialize data
	var value uint64
	if err := json.Unmarshal([]byte(data), &value); err != nil {
		log.Printf("Invalid data, cannot deserialize")
		return false, errors.Wrapf(err, "Invalid payload data (eventID: %s, source: %s)", in.ID, in.Source)
	}

	// Process event
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

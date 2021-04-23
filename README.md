
# Start producer:
```dapr run -a producer -d ./components go run producer/main.go```

# Start consumer:
## HTTP:
```dapr run -a consumer -P http -p 27015 -d ./components go run consumer/main.go```
## or gRPC:
```dapr run -a consumer -P gRPC -p 27015 -d ./components go run consumer/main.go -- -consumermode gRPC```

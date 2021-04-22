
# Start producer:
```dapr run -a producer go run producer/main.go```

# Start consumer:
## HTTP:
```dapr run -a consumer -P http -p 27015 go run consumer/main.go```
## or gRPC:
```dapr run -a consumer -P gRPC -p 27015 go run consumer/main.go gRPC```

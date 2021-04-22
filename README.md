
# Start producer:
dapr run -a producer -d ./components go run producer/main.go

# Start Consumer:
dapr run -a consumer -P http -p 27015 -d ./components go run consumer/main.go

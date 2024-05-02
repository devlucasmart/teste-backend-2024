package main

import (
	"ms-go/consumers"
	"ms-go/router"
)

func main() {
	go consumers.StartKafkaConsumer()
	router.Run()
}

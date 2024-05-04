package products

import (
	"context"
	"encoding/json"
	"ms-go/app/helpers"
	"ms-go/app/models"
	"ms-go/db"
	"net/http"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	KafkaServer = "kafka:29092"
	KafkaTopic  = "go-to-rails"
)

func Create(data models.Product, isAPI bool) (*models.Product, error) {
	if data.ID == 0 {
		var max models.Product
		opts := options.FindOne()
		opts.SetSort(bson.D{{Key: "created_at", Value: -1}})
		db.Connection().FindOne(context.TODO(), bson.D{}, opts).Decode(&max)
		data.ID = max.ID + 1
	}

	if err := data.Validate(); err != nil {
		return nil, &helpers.GenericError{Msg: err.Error(), Code: http.StatusUnprocessableEntity}
	}

	data.CreatedAt = time.Now()
	data.UpdatedAt = data.CreatedAt

	_, err := db.Connection().InsertOne(context.TODO(), data)
	if err != nil {
		return nil, &helpers.GenericError{Msg: err.Error(), Code: http.StatusInternalServerError}
	}

	// Não desconecte o banco de dados aqui, pois podemos precisar dele mais tarde

	if isAPI {
		product := models.Product{
			ID:          data.ID,
			Name:        data.Name,
			Brand:       data.Brand,
			Price:       data.Price,
			Description: data.Description,
			Stock:       data.Stock,
			CreatedAt:   data.CreatedAt,
			UpdatedAt:   data.UpdatedAt,
		}

		producer, err := kafka.NewProducer(&kafka.ConfigMap{
			"bootstrap.servers": KafkaServer,
			"client.id":         "ms-go",
		})
		if err != nil {
			// Handle error creating Kafka producer
			return nil, err
		}
		defer producer.Close()

		topic := KafkaTopic
		value, err := json.Marshal(product)
		if err != nil {
			// Handle error marshalling product to JSON
			return nil, err
		}

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
		}, nil)

		if err != nil {
			// Handle error producing message to Kafka topic
			return nil, err
		}
	}

	return &data, nil
}

package consumers

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Product struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Brand       string  `json:"brand"`
	Price       float64 `json:"price"`
	Description string  `json:"description"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
	Stock       int     `json:"stock"`
}

func StartKafkaConsumer() {
	log.Println("Iniciando consumidor Kafka...")
	// Configuração do consumidor Kafka
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_0_0_0

	// Lista de brokers Kafka
	brokers := []string{"kafka:29092"}

	// Create a new Kafka consumer
	client, err := sarama.NewConsumerGroup(brokers, "group_id", config)
	if err != nil {
		log.Fatalf("Erro ao criar cliente Kafka: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("Erro ao fechar cliente Kafka: %v", err)
		}
	}()

	// Tópico que o consumidor irá consumir
	topics := []string{"rails-to-go"}

	// Canal para sinais de interrupção
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Configuração do cliente MongoDB
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	mongoClient, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			log.Printf("Erro ao desconectar do MongoDB: %v", err)
		}
	}()

	// Selecionar o banco de dados e coleção MongoDB
	database := mongoClient.Database("teste_backend")
	collection := database.Collection("products")

	// Armazenar o último offset consumido
	var offsetMu sync.Mutex
	var lastOffset int64

	// Função de manipulação de mensagem do Kafka
	handler := &KafkaHandler{
		Collection: collection,
		OffsetMu:   &offsetMu,
		LastOffset: &lastOffset,
	}

	// Iniciar o consumidor Kafka
	go func() {
		for {
			err := client.Consume(context.Background(), topics, handler)
			if err != nil {
				log.Printf("Erro ao consumir mensagens do Kafka: %v", err)
			}
		}
	}()

	// Esperar por sinais de interrupção
	<-signals
	log.Println("Encerrando consumidor Kafka...")
}

// KafkaHandler é uma estrutura que implementa a interface sarama.ConsumerGroupHandler
type KafkaHandler struct {
	Collection *mongo.Collection
	OffsetMu   *sync.Mutex
	LastOffset *int64
}

// Setup é chamada antes da inicialização do consumidor
func (h *KafkaHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup é chamada quando o consumidor sai
func (h *KafkaHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim é chamada quando novas mensagens são recebidas
func (h *KafkaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Mensagem recebida: %s\n", message.Value)

		// Verificar se o offset da mensagem é maior que o último offset consumido
		h.OffsetMu.Lock()
		if message.Offset >= *h.LastOffset {
			// Decodificar a mensagem JSON para a estrutura Product
			var product Product
			err := json.Unmarshal(message.Value, &product)
			if err != nil {
				log.Println("Erro ao decodificar mensagem JSON:", err)
				// Imprimir o conteúdo da mensagem para entender o formato
				log.Println("Conteúdo da mensagem:", string(message.Value))
				continue
			}

			// Salvar o produto no MongoDB
			_, err = h.Collection.InsertOne(context.Background(), product)
			if err != nil {
				log.Println("Erro ao salvar produto no MongoDB:", err)
				continue
			}
			log.Println("Produto salvo no MongoDB com sucesso!")

			// Atualizar o último offset consumidor
			*h.LastOffset = message.Offset
		}
		h.OffsetMu.Unlock()

		session.MarkMessage(message, "")
	}

	return nil
}

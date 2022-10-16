package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

	"go.opentelemetry.io/contrib/instrumentation/github.com/Shopify/sarama/otelsarama"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	// KafkaTopic name.
	KafkaTopic = "order-topic"
)

// InitTracer creates and registers globally a new TracerProvider.
func InitTracer() (*sdktrace.TracerProvider, error) {
	secureOption := otlptracegrpc.WithInsecure()

	exporter, err := otlptrace.New(
		context.Background(),
		otlptracegrpc.NewClient(
			secureOption,
		),
	)

	if err != nil {
		return nil, err
	}

	resources, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", "invoice-serivce"),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resources),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
	return tp, nil
}

var (
	brokers = flag.String("brokers", os.Getenv("BOOTSTRAP_SERVERS"), "The Kafka brokers to connect to, as a comma separated list")
)

func main() {
	tp, err := InitTracer()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()
	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := startConsumerGroup(ctx, brokerList); err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
}

func startConsumerGroup(ctx context.Context, brokerList []string) error {
	consumerGroupHandler := Consumer{}
	// Wrap instrumentation
	handler := otelsarama.WrapConsumerGroupHandler(&consumerGroupHandler)

	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokerList, "otel-kafka-example-invoice", config)
	if err != nil {
		return fmt.Errorf("starting consumer group: %w", err)
	}

	err = consumerGroup.Consume(ctx, []string{KafkaTopic}, handler)
	if err != nil {
		return fmt.Errorf("consuming via handler: %w", err)
	}
	return nil
}

func printMessage(msg *sarama.ConsumerMessage) {
	// Extract tracing info from message
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), otelsarama.NewConsumerMessageCarrier(msg))

	tr := otel.Tracer("consumer")
	_, span := tr.Start(ctx, "invoicing", trace.WithAttributes(
		semconv.MessagingOperationProcess,
	))
	defer span.End()

	// Emulate Work loads
	time.Sleep(1 * time.Second)

	log.Println("Successful to read message: ", string(msg.Value))
}

// Consumer represents a Sarama consumer group consumer.
type Consumer struct {
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		printMessage(message)
		session.MarkMessage(message, "")
	}

	return nil
}

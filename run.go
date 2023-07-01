package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/seancaffery/dash-kafka/kafka"
	"golang.org/x/sync/errgroup"
)

// echoProcessor implements MessageProcessorFunc and outputs the message and any headers to STDOUT
func echoProcessor(ctx context.Context, message kafka.ConsumerMessage) error {
	fmt.Printf("message: %+v, headers: %+v\n", string(message.Message), message.Headers)
	return nil
}

// intetrceptor an example ConsumerInterceptor showing how pre and post processing of messages can be added
func interceptor(p kafka.MessageProcessor) kafka.MessageProcessor {
	return kafka.MessageProcessorFunc(func(ctx context.Context, message kafka.ConsumerMessage) error {
		fmt.Println("before")
		err := p.ProcessMessage(ctx, message)
		fmt.Println("after")
		return err
	})
}

func stats(json string) {
	fmt.Println(json)
}

func logs(level int, fac string, buf string) {
	fmt.Println(buf)
}

func main() {
	var f kafka.MessageProcessorFunc = echoProcessor
	rand.Seed(time.Now().Unix())
	grp := rand.Int63()
	config := kafka.ConsumerConfiguration{
		SharedConfiguration: kafka.SharedConfiguration{
			Brokers:            []string{"127.0.0.1:9092"},
			StatisticsCallback: stats,
			// set StatisticsInterval >0 to enable stats callback
			StatisticsInterval: 0,
			LogCallback:        logs,
		},
		GroupID: strconv.FormatInt(grp, 10),
	}
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 10)
	c, err := kafka.NewConsumer([]string{"cooltopic"}, config, f, errChan)
	if err != nil {
		panic(err)
	}

	c.AddConsumerInterceptor(interceptor)

	producerConfig := kafka.ProducerConfiguration{
		SharedConfiguration: kafka.SharedConfiguration{
			Brokers: []string{"127.0.0.1:9092"},
			// set StatisticsInterval >0 to enable stats callback
			StatisticsInterval: 0,
			StatisticsCallback: stats,
			LogCallback:        logs,
		},
	}
	p, _ := kafka.NewProducer(producerConfig)

	g, ctx := errgroup.WithContext(ctx)
	// start consumer and producer
	g.Go(func() error { return c.Start(ctx) })
	g.Go(func() error { return p.Start(ctx) })
	// listen for fatal errors
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errChan:
			fmt.Printf("ERROR: %s\n", err)
		}
		return nil
	})
	// produce a message once per second
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.NewTicker(1 * time.Second).C:
				p.Produce(kafka.ProducerMessage{
					Key:     []byte("hey"),
					Message: []byte("lololol"),
					TopicPartition: &kafka.TopicPartition{
						Topic: "cooltopic",
					},
				})
			}
		}
	})

	go func() {
		err := g.Wait()
		if err != nil {
			fmt.Printf("ERROR: %s\n", err)
		}
		cancel()
	}()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint
	cancel()
	time.Sleep(5000 * time.Millisecond)
}

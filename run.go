package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/seancaffery/dash-kafka/kafka"
	"golang.org/x/sync/errgroup"
)

// echoProcessor implements MessageProcessorFunc and outputs the message and any headers to STDOUT
func echoProcessor(ctx context.Context, message kafka.ConsumerMessage) error {
	fmt.Printf("message: %+v, headers: %+v\n", string(message.Message), message.Headers)
	return nil
}

func main() {
	var f kafka.MessageProcessorFunc = echoProcessor
	config := kafka.Configuration{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "cool_group",
	}
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 10)
	c, err := kafka.NewConsumer([]string{"cooltopic"}, config, f, errChan)
	if err != nil {
		panic(err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return c.Start(ctx) })
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errChan:
			fmt.Printf("ERROR: %s\n", err)
		}
		return nil
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

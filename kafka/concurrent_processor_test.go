package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func newMessage(message string) *ConsumerMessage {
	return &ConsumerMessage{
		Message: []byte(message),
		Err:     make(chan error),
	}
}

func newMessageKey(key string) *ConsumerMessage {
	return &ConsumerMessage{
		Key: []byte(key),
		Err: make(chan error),
	}
}

func TestCallsProcessorOnEnqueuedMessage(t *testing.T) {
	var processedMessage string
	processorCalled := false
	mp := MessageProcessorFunc(func(ctx context.Context, message ConsumerMessage) error {
		processorCalled = true
		processedMessage = string(message.Message)
		return nil
	})

	processor := newConcurrentProcessor(1, mp)
	processor.Start(context.Background())

	message := newMessage("hi")
	processor.Enqueue(context.Background(), message)
	<-message.Err

	assert.True(t, processorCalled, "Expected message processor to be called")
	assert.Equal(t, "hi", processedMessage)
}

func TestEnqueue(t *testing.T) {
	processorCalled := false
	mp := MessageProcessorFunc(func(ctx context.Context, message ConsumerMessage) error {
		processorCalled = true
		return nil
	})
	message := newMessage("hi")

	t.Run("doens't enqueue on canceled processor context", func(t *testing.T) {
		processorCalled = false

		processorCtx, cancel := context.WithCancel(context.Background())
		processor := newConcurrentProcessor(1, mp)
		processor.Start(processorCtx)
		cancel()

		processor.Enqueue(context.Background(), message)

		assert.False(t, processorCalled, "Expected message processor to not be called")
	})

	t.Run("doens't enqueue on canceled message context", func(t *testing.T) {
		processorCalled := false

		messageCtx, cancel := context.WithCancel(context.Background())
		processor := newConcurrentProcessor(1, mp)
		processor.Start(context.Background())
		cancel()

		processor.Enqueue(messageCtx, message)

		assert.False(t, processorCalled, "Expected message processor to not be called")
	})

	t.Run("writes messages to serialization queue in recieved order", func(t *testing.T) {
		processorCalled := false

		mp := MessageProcessorFunc(func(ctx context.Context, message ConsumerMessage) error {
			processorCalled = true
			return nil
		})

		processor := newConcurrentProcessor(1, mp)
		processor.Start(context.Background())

		message1 := newMessage("one")
		message2 := newMessage("two")
		message3 := newMessage("three")
		expectedResult := []string{string(message1.Message), string(message2.Message), string(message3.Message)}
		result := []string{}

		wg := errgroup.Group{}
		wg.Go(func() error {
			for msg := range processor.Serialization() {
				result = append(result, string(msg.Message))
			}
			return nil
		})

		processor.Enqueue(context.Background(), message1)
		processor.Enqueue(context.Background(), message2)
		processor.Enqueue(context.Background(), message3)
		<-message3.Err
		processor.Shutdown()
		wg.Wait()

		assert.True(t, processorCalled, "Expected message processor to be called")
		assert.Equal(t, expectedResult, result)
	})

	t.Run("does not concurrently process messages with the same key", func(t *testing.T) {
		processorCalled := false

		processedMessageKeys := []string{}
		mp := MessageProcessorFunc(func(ctx context.Context, message ConsumerMessage) error {
			processorCalled = true
			processedMessageKeys = append(processedMessageKeys, string(message.Key))
			return nil
		})

		processor := newConcurrentProcessor(2, mp)
		processor.Start(context.Background())

		message1 := newMessageKey("one")
		message2 := newMessageKey("one")
		message3 := newMessageKey("one")
		message4 := newMessageKey("two")
		expectedResult := []string{string(message1.Key), string(message4.Key), string(message2.Key), string(message3.Key)}
		result := []string{}

		go func() {
			for msg := range processor.Serialization() {
				result = append(result, string(msg.Key))
			}
		}()
		expectedSerializedResult := []string{string(message1.Key), string(message2.Key), string(message2.Key), string(message4.Key)}

		processor.Enqueue(context.Background(), message1)
		processor.Enqueue(context.Background(), message2)
		processor.Enqueue(context.Background(), message3)
		processor.Enqueue(context.Background(), message4)
		<-message1.Err
		<-message2.Err
		<-message3.Err
		<-message4.Err
		processor.Shutdown()

		assert.True(t, processorCalled, "Expected message processor to be called")
		assert.Equal(t, expectedSerializedResult, result)
		assert.Equal(t, expectedResult, processedMessageKeys)
	})
}

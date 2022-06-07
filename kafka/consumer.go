package kafka

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
	"unsafe"

	"golang.org/x/sync/errgroup"
)

type Headers map[string][]byte

type ConsumerMessage struct {
	Key            []byte
	Message        []byte
	Headers        Headers
	TopicPartition *TopicPartition
	Err            error
}

type TopicPartition struct {
	Topic     string
	Partition int
	Offset    int64
}

type MessageProcessor interface {
	ProcessMessage(ctx context.Context, messsage ConsumerMessage) error
}

type MessageProcessorFunc func(ctx context.Context, message ConsumerMessage) error

func (f MessageProcessorFunc) ProcessMessage(ctx context.Context, message ConsumerMessage) error {
	return f(ctx, message)
}

type Consumer interface {
}

type consumer struct {
	handle          *handle
	topics          []string
	processor       MessageProcessor
	errChan         chan<- error
	partitionWg     errgroup.Group
	ctx             context.Context
	partitionCtx    context.Context
	partitionCancel context.CancelFunc
}

type handle struct {
	client *C.rd_kafka_t
	queue  *C.rd_kafka_queue_t
}

//export goRebalance
func goRebalance(kafkaHandle *C.rd_kafka_t, cErr C.rd_kafka_resp_err_t,
	partitions *C.rd_kafka_topic_partition_list_t, opaque unsafe.Pointer) {
	switch cErr {
	case C.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
		gConsumer.partitionCtx, gConsumer.partitionCancel = context.WithCancel(gConsumer.ctx)
		C.rd_kafka_assign(kafkaHandle, partitions)

		goPartitions := unsafe.Slice(partitions.elems, partitions.cnt)
		for _, partition := range goPartitions {
			tp := TopicPartition{
				Topic:     C.GoString(partition.topic),
				Partition: int(partition.partition),
				Offset:    int64(partition.offset),
			}
			gConsumer.partitionWg.Go(func() error {
				return gConsumer.readPartition(gConsumer.partitionCtx, tp)
			})
		}
	case C.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
		gConsumer.partitionCancel()
		gConsumer.partitionWg.Wait()
		C.rd_kafka_assign(kafkaHandle, nil)
	default:
		C.rd_kafka_assign(kafkaHandle, nil)
	}
}

// kinda gross, but easy for now
// share consumer with C to factor this away
var gConsumer *consumer

func NewConsumer(topics []string, goConf ConsumerConfiguration, processor MessageProcessor, errChan chan<- error) (*consumer, error) {
	consumer := &consumer{
		handle:    &handle{},
		topics:    topics,
		processor: processor,
		errChan:   errChan,
	}
	gConsumer = consumer

	conf, err := goConf.setup()
	if err != nil {
		return nil, fmt.Errorf("failed to configure consumer: %+v", err)
	}

	cErr := C.malloc(C.size_t(128))
	consumer.handle.client = C.rd_kafka_new(C.RD_KAFKA_CONSUMER, conf, (*C.char)(cErr), 128)
	if consumer.handle.client == nil {
		return nil, fmt.Errorf("failed to create new consumer: %s", C.GoString((*C.char)(cErr)))
	}
	C.free(cErr)

	// Configuration object is now owned, and freed, by the rd_kafka_t instance.
	conf = nil

	return consumer, nil
}

func (c *consumer) readPartition(ctx context.Context, tp TopicPartition) error {
	var q *C.struct_rd_kafka_queue_s = C.rd_kafka_queue_get_partition(c.handle.client, C.CString(tp.Topic), C.int(tp.Partition))
	// disable forwarding to common consumer queue for subscribed partitions
	C.rd_kafka_queue_forward(q, nil)

	for {
		select {
		case <-ctx.Done():
			// TODO: clean up properly here
			C.rd_kafka_queue_destroy(q)
			return nil
		default:
			event := C.rd_kafka_queue_poll(q, C.int(100))
			eventType := C.rd_kafka_event_type(event)

			switch eventType {
			case C.RD_KAFKA_EVENT_NONE:
				break
			case C.RD_KAFKA_EVENT_FETCH:
				cmessage := C.rd_kafka_event_message_next(event)
				if cmessage != nil {
					if cmessage.err != C.RD_KAFKA_RESP_ERR_NO_ERROR {
						c.errChan <- fmt.Errorf(C.GoString(C.rd_kafka_err2str(cmessage.err)))
						C.rd_kafka_message_destroy(cmessage)
						continue
					}
					message, err := c.cToGoMessage(cmessage)
					if err != nil {
						fmt.Printf("error: %+v", err)
						continue
					}

					c.processor.ProcessMessage(ctx, *message)
					C.rd_kafka_message_destroy(cmessage)
				}
			default:
				panic(fmt.Sprintf("unkonwn event type: %+v", eventType))
			}
		}
	}

	C.rd_kafka_queue_destroy(q)
	return nil
}

func (c *consumer) Start(ctx context.Context) error {
	c.ctx = ctx
	// Subscribe to the list of topics
	subscription := C.rd_kafka_topic_partition_list_new(C.int(len(c.topics)))
	for _, t := range c.topics {
		C.rd_kafka_topic_partition_list_add(subscription, C.CString(t), C.RD_KAFKA_PARTITION_UA)
	}

	cErr := C.rd_kafka_subscribe(c.handle.client, subscription)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		C.rd_kafka_topic_partition_list_destroy(subscription)
		C.rd_kafka_destroy(c.handle.client)
		return fmt.Errorf(C.GoString(C.rd_kafka_err2str(cErr)))
	}

	C.rd_kafka_topic_partition_list_destroy(subscription)
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("shutting down")
			C.rd_kafka_consumer_close(c.handle.client)
			C.rd_kafka_destroy(c.handle.client)
			return nil
		default:
			// general callbacks - logs, stats, etc
			C.rd_kafka_poll(c.handle.client, 100)
			// consumer specific callbacks
			C.rd_kafka_consumer_poll(c.handle.client, 100)
		}
	}
}

func (c *consumer) cToGoHeaders(cMessage *C.rd_kafka_message_t) (Headers, error) {
	var cHeaders *C.struct_rd_kafka_headers_s
	cErr := C.rd_kafka_message_headers(cMessage, &cHeaders)
	if cErr != C.RD_KAFKA_RESP_ERR_NO_ERROR {
		// Message doesn't have headers
		if cErr == C.RD_KAFKA_RESP_ERR__NOENT {
			return nil, nil
		}
		return nil, fmt.Errorf(C.GoString(C.rd_kafka_err2str(cErr)))
	}

	count := C.rd_kafka_header_cnt(cHeaders)

	size := C.size_t(0)
	var name *C.char
	var value unsafe.Pointer
	headers := Headers{}

	for i := C.size_t(0); i < count; i++ {
		C.rd_kafka_header_get_all(cHeaders, i, &name, &value, &size)
		headers[C.GoString(name)] = C.GoBytes(value, C.int(size))
	}

	return headers, nil
}

func (c *consumer) cToGoMessage(cMessage *C.rd_kafka_message_t) (*ConsumerMessage, error) {
	topicPartition := &TopicPartition{
		Partition: int(cMessage.partition),
		Offset:    int64(cMessage.offset),
	}
	headers, err := c.cToGoHeaders(cMessage)
	if err != nil {
		return nil, err
	}
	messageBytes := C.GoBytes(unsafe.Pointer(cMessage.payload), C.int(cMessage.len))
	return &ConsumerMessage{
		Message:        messageBytes,
		TopicPartition: topicPartition,
		Headers:        headers,
	}, nil
}

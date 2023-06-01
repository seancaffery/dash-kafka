package kafka

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"fmt"
	"runtime/cgo"
	"unsafe"

	"golang.org/x/sync/errgroup"
)

type Headers map[string][]byte

type ConsumerMessage struct {
	Key            []byte
	Message        []byte
	Headers        Headers
	TopicPartition *TopicPartition
	Err            chan error
	cMessageHandle *C.rd_kafka_message_t
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

type ConsumerInterceptor func(MessageProcessor) MessageProcessor

type Consumer interface {
}

type assignment struct {
	ctx      context.Context
	cancel   context.CancelFunc
	errGroup errgroup.Group
}

type consumer struct {
	handle      *handle
	topics      []string
	processor   MessageProcessor
	errChan     chan<- error
	ctx         context.Context
	cMap        map[string]interface{}
	assignments map[string]*assignment
}

type handle struct {
	client *C.rd_kafka_t
	queue  *C.rd_kafka_queue_t
}

//export goRebalance
func goRebalance(kafkaHandle *C.rd_kafka_t, cErr C.rd_kafka_resp_err_t,
	partitions *C.rd_kafka_topic_partition_list_t, opaque unsafe.Pointer) {
	cMap := cgo.Handle(opaque).Value().(map[string]interface{})
	consumerRef := cMap["consumer"].(*consumer)
	goPartitions := unsafe.Slice(partitions.elems, partitions.cnt)

	rebalanceType := C.GoString(C.rd_kafka_rebalance_protocol(kafkaHandle))
	cooperative := false
	if rebalanceType == "COOPERATIVE" {
		cooperative = true
	}

	switch cErr {
	case C.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
		if cooperative {
			C.rd_kafka_incremental_assign(kafkaHandle, partitions)
		} else {
			C.rd_kafka_assign(kafkaHandle, partitions)
		}

		for _, partition := range goPartitions {
			topic := C.GoString(partition.topic)
			assignmentName := fmt.Sprintf("%s%d", topic, int(partition.partition))
			tp := TopicPartition{
				Topic:     topic,
				Partition: int(partition.partition),
				Offset:    int64(partition.offset),
			}
			partitionCtx, partitionCancel := context.WithCancel(consumerRef.ctx)
			assignment := &assignment{
				ctx:    partitionCtx,
				cancel: partitionCancel,
			}
			consumerRef.assignments[assignmentName] = assignment
			processor := newConcurrentProcessor(partitionCtx, 3, consumerRef.processor)
			assignment.errGroup.Go(func() error {
				return consumerRef.readPartition(partitionCtx, tp, processor)
			})

			assignment.errGroup.Go(func() error {
				consumerRef.partitionMessageListener(partitionCtx, processor)
				return nil
			})
			processor.Start(partitionCtx)
		}
	case C.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
		for _, partition := range goPartitions {
			topic := C.GoString(partition.topic)
			assignmentName := fmt.Sprintf("%s%d", topic, int(partition.partition))
			consumerRef.assignments[assignmentName].cancel()
			consumerRef.assignments[assignmentName].errGroup.Wait()
		}

		if cooperative {
			C.rd_kafka_incremental_unassign(kafkaHandle, partitions)
		} else {
			C.rd_kafka_assign(kafkaHandle, nil)
		}
	default:
		C.rd_kafka_assign(kafkaHandle, nil)
	}
}

const KAFKA_FREE_STATS_JSON int = 0

//export goStatsCb
func goStatsCb(kafkaHandle *C.rd_kafka_t, json *C.char, len C.size_t, opaque unsafe.Pointer) int {
	cMap := cgo.Handle(opaque).Value().(map[string]interface{})
	statsFunc := cMap["stats"].(StatisticsCallback)
	statsFunc(C.GoString(json))
	return KAFKA_FREE_STATS_JSON
}

//export goLogCb
func goLogCb(kafkaHandle *C.rd_kafka_t, level C.int, fac *C.char, buf *C.char) {
	opaque := C.rd_kafka_opaque(kafkaHandle)
	cMap := cgo.Handle(opaque).Value().(map[string]interface{})
	logger := cMap["logger"].(LogCallback)
	logger(int(level), C.GoString(fac), C.GoString(buf))
}

func NewConsumer(topics []string, goConf ConsumerConfiguration, processor MessageProcessor, errChan chan<- error) (*consumer, error) {
	cMap := map[string]interface{}{}
	assignments := map[string]*assignment{}
	consumer := &consumer{
		handle:      &handle{},
		topics:      topics,
		processor:   processor,
		errChan:     errChan,
		cMap:        cMap,
		assignments: assignments,
	}
	cMap["consumer"] = consumer
	cMap["stats"] = goConf.StatisticsCallback
	cMap["logger"] = goConf.LogCallback

	conf, err := goConf.setup(cMap)
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

func (c *consumer) AddConsumerInterceptor(interceptors ...ConsumerInterceptor) {
	for _, interceptor := range interceptors {
		c.processor = interceptor(c.processor)
	}
}

func (c *consumer) commitMessage(message *ConsumerMessage) {
	C.rd_kafka_commit_message(c.handle.client, message.cMessageHandle, C.int(0))
	C.rd_kafka_message_destroy(message.cMessageHandle)
}

func (c *consumer) partitionMessageListener(ctx context.Context, processor *concurrentProcessor) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-processor.Serialization():

			if ctx.Err() != nil {
				return
			}

			err := <-message.Err
			if err != nil {
				return
			}

			c.commitMessage(message)
		}
	}
}

func (c *consumer) readPartition(ctx context.Context, tp TopicPartition, processor *concurrentProcessor) error {
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
					message, err := cToGoMessage(cmessage)
					if err != nil {
						fmt.Printf("error: %+v", err)
						continue
					}

					message.cMessageHandle = cmessage
					processor.Enqueue(ctx, message)
				}
			default:
				panic(fmt.Sprintf("unkonwn event type: %+v", eventType))
			}
		}
	}
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
			fmt.Println("shutting down consumer")
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

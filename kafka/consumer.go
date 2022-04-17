package kafka

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>

extern void rebalanceCgo(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque);
*/
import "C"
import (
	"context"
	"fmt"
	"strings"
	"unsafe"
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
	handle    *handle
	topics    []string
	processor MessageProcessor
	errChan   chan<- error
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
		C.rd_kafka_assign(kafkaHandle, partitions)
	case C.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
		C.rd_kafka_assign(kafkaHandle, partitions)
	default:
		C.rd_kafka_assign(kafkaHandle, nil)
	}
}

type Configuration struct {
	Brokers []string
	GroupID string
	// Configuration passed directly to librdkafka
	// Provides an escape hatch for configuration not directly exposed. Prefer to add explicit configuration.
	LibKafkaConf map[string]interface{}
}

func NewConsumer(topics []string, goConf Configuration, processor MessageProcessor, errChan chan<- error) (*consumer, error) {
	consumer := &consumer{
		handle:    &handle{},
		topics:    topics,
		processor: processor,
		errChan:   errChan,
	}

	conf, err := consumer.setupConf(goConf)
	if err != nil {
		return nil, fmt.Errorf("Failed to configure consumer: %+v", err)
	}

	cErr := C.malloc(C.size_t(128))
	consumer.handle.client = C.rd_kafka_new(C.RD_KAFKA_CONSUMER, conf, (*C.char)(cErr), 128)
	if consumer.handle.client == nil {
		return nil, fmt.Errorf("Failed to create new consumer: %s\n", C.GoString((*C.char)(cErr)))
	}
	C.free(cErr)

	// Configuration object is now owned, and freed, by the rd_kafka_t instance.
	conf = nil

	return consumer, nil
}

func (c *consumer) setupConf(goConf Configuration) (*C.struct_rd_kafka_conf_s, error) {
	allBrokers := strings.Join(goConf.Brokers, ",")
	brokers := C.CString(allBrokers)
	groupID := C.CString(goConf.GroupID)
	cErr := C.malloc(C.size_t(128))

	conf := C.rd_kafka_conf_new()

	rdKafkaKeyMap := map[string]interface{}{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
		"socket.nagle.disable": true,
	}

	for k, v := range rdKafkaKeyMap {
		if C.rd_kafka_conf_set(conf, C.CString(k), v.(*C.char), (*C.char)(cErr), 128) != C.RD_KAFKA_CONF_OK {
			C.rd_kafka_conf_destroy(conf)
			goErrString := C.GoString((*C.char)(cErr))
			C.free(cErr)
			return nil, fmt.Errorf("could not set %s: %+v", k, goErrString)
		}
	}

	for k, v := range goConf.LibKafkaConf {
		if C.rd_kafka_conf_set(conf, C.CString(k), v.(*C.char), (*C.char)(cErr), 128) != C.RD_KAFKA_CONF_OK {
			C.rd_kafka_conf_destroy(conf)
			goErrString := C.GoString((*C.char)(cErr))
			C.free(cErr)
			return nil, fmt.Errorf("could not set %s: %+v", k, goErrString)
		}
	}

	// Go being Go https://github.com/golang/go/issues/19835
	C.rd_kafka_conf_set_rebalance_cb(conf, (*[0]byte)(C.rebalanceCgo))

	return conf, nil
}

func (c *consumer) Start(ctx context.Context) error {
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
			C.rd_kafka_poll(c.handle.client, 100)
			cmessage := C.rd_kafka_consumer_poll(c.handle.client, 100)
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

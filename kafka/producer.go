package kafka

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>

extern void drCbCgo(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
*/
import "C"
import (
	"context"
	"fmt"
	"strings"
	"unsafe"
)

type Producer struct{}

type producer struct {
	handle *handle
}

type ProducerMessage struct {
	Message []byte
	Key     []byte
}

//export goDrCb
func goDrCb(kafkaHandle *C.rd_kafka_t, kafkaMessage *C.rd_kafka_message_t, opaque unsafe.Pointer) {
	// * This callback is called exactly once per message, indicating if
	// * the message was succesfully delivered
	// * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
	// * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).

	message, err := cToGoMessage(kafkaMessage)
	if err != nil {
		fmt.Printf("error: %+v\n", message)
	}
	fmt.Printf("DR: %s %s\n", string(message.Key), string(message.Message))
}

func cToGoMessage(cMessage *C.rd_kafka_message_t) (*ConsumerMessage, error) {
	topicPartition := &TopicPartition{
		Partition: int(cMessage.partition),
		Offset:    int64(cMessage.offset),
	}
	// headers, err := cToGoHeaders(cMessage)
	// if err != nil {
	// 	return nil, err
	// }
	messageBytes := C.GoBytes(unsafe.Pointer(cMessage.payload), C.int(cMessage.len))
	return &ConsumerMessage{
		Message:        messageBytes,
		TopicPartition: topicPartition,
		// Headers:        headers,
	}, nil
}

func NewProducer(goConf Configuration) (*producer, error) {
	producer := &producer{
		handle: &handle{},
	}

	conf, err := producer.setupConf(goConf)
	if err != nil {
		return nil, fmt.Errorf("failed to configure consumer: %+v", err)
	}

	cErr := C.malloc(C.size_t(128))
	producer.handle.client = C.rd_kafka_new(C.RD_KAFKA_PRODUCER, conf, (*C.char)(cErr), 128)
	if producer.handle.client == nil {
		return nil, fmt.Errorf("failed to create new consumer: %s", C.GoString((*C.char)(cErr)))
	}
	C.free(cErr)

	// Configuration object is now owned, and freed, by the rd_kafka_t instance.
	conf = nil

	return producer, nil
}

func (c *producer) setupConf(goConf Configuration) (*C.struct_rd_kafka_conf_s, error) {
	allBrokers := strings.Join(goConf.Brokers, ",")
	cErr := C.malloc(C.size_t(128))

	conf := C.rd_kafka_conf_new()

	rdKafkaKeyMap := map[string]interface{}{
		"bootstrap.servers": allBrokers,
		// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
		"socket.nagle.disable": true,
	}

	for k, v := range rdKafkaKeyMap {
		strVal, err := stringify(v)
		if err != nil {
			return nil, err
		}
		if C.rd_kafka_conf_set(conf, C.CString(k), C.CString(strVal), (*C.char)(cErr), 128) != C.RD_KAFKA_CONF_OK {
			C.rd_kafka_conf_destroy(conf)
			goErrString := C.GoString((*C.char)(cErr))
			C.free(cErr)
			return nil, fmt.Errorf("could not set %s: %+v", k, goErrString)
		}
	}

	for k, v := range goConf.LibKafkaConf {
		strVal, err := stringify(v)
		if err != nil {
			return nil, err
		}
		if C.rd_kafka_conf_set(conf, C.CString(k), C.CString(strVal), (*C.char)(cErr), 128) != C.RD_KAFKA_CONF_OK {
			C.rd_kafka_conf_destroy(conf)
			goErrString := C.GoString((*C.char)(cErr))
			C.free(cErr)
			return nil, fmt.Errorf("could not set %s: %+v", k, goErrString)
		}
	}

	// * The callback is only triggered from rd_kafka_poll() and
	// * rd_kafka_flush().
	// Go being Go https://github.com/golang/go/issues/19835
	C.rd_kafka_conf_set_dr_msg_cb(conf, (*[0]byte)(C.drCbCgo))

	return conf, nil
}

func (p *producer) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("shutting down")
			// send any queued messages to the broker
			C.rd_kafka_flush(p.handle.client, C.int(1000))
			C.rd_kafka_destroy(p.handle.client)
			return nil
		default:
			// general callbacks - logs, stats, delivery reports etc
			C.rd_kafka_poll(p.handle.client, 100)
		}
	}
	return nil
}

func (p *producer) Produce(tp TopicPartition, message ProducerMessage) error {
	partition := C.int(tp.Partition)
	topic := C.rd_kafka_topic_new(p.handle.client, C.CString(tp.Topic), nil)
	result := C.rd_kafka_produce(
		topic,
		partition,
		C.RD_KAFKA_MSG_F_COPY,
		unsafe.Pointer(&message.Message[0]),
		C.size_t(len(message.Message)),
		unsafe.Pointer(&message.Key[0]),
		C.size_t(len(message.Key)),
		nil)

	C.rd_kafka_errno2err(result)

	C.rd_kafka_topic_destroy(topic)

	return nil
}

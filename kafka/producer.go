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

func NewProducer(goConf ProducerConfiguration) (*producer, error) {
	producer := &producer{
		handle: &handle{},
	}

	conf, err := goConf.setup()
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

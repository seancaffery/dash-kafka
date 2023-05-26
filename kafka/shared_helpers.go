package kafka

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func cToGoHeaders(cMessage *C.rd_kafka_message_t) (Headers, error) {
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

func cToGoMessage(cMessage *C.rd_kafka_message_t) (*ConsumerMessage, error) {
	topicPartition := &TopicPartition{
		Partition: int(cMessage.partition),
		Offset:    int64(cMessage.offset),
	}
	headers, err := cToGoHeaders(cMessage)
	if err != nil {
		return nil, err
	}
	messageBytes := C.GoBytes(unsafe.Pointer(cMessage.payload), C.int(cMessage.len))
	keyBytes := C.GoBytes(unsafe.Pointer(cMessage.key), C.int(cMessage.key_len))
	return &ConsumerMessage{
		Key:            keyBytes,
		Message:        messageBytes,
		TopicPartition: topicPartition,
		Headers:        headers,
		Err:            make(chan error, 1),
	}, nil
}

package kafka

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>

extern void rebalanceCgo(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque);
extern void drCbCgo(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
*/
import "C"
import (
	"fmt"
	"strings"
	"unsafe"
)

type SharedConfiguration struct {
	Brokers []string
	// Configuration passed directly to librdkafka
	// Provides an escape hatch for configuration not directly exposed. Prefer to add explicit configuration.
	LibKafkaConf map[string]interface{}
}

type ConsumerConfiguration struct {
	SharedConfiguration

	GroupID string
}

type ProducerConfiguration struct {
	SharedConfiguration
}

func stringify(in interface{}) (string, error) {
	value := ""
	switch x := in.(type) {
	case bool:
		if x {
			value = "true"
		} else {
			value = "false"
		}
	case int:
		value = fmt.Sprintf("%d", x)
	case string:
		value = x
	default:
		return "", fmt.Errorf("invalid config type %T, %v", in, in)
	}

	return value, nil
}

func setCConf(values map[string]interface{}, kafkaConf *C.struct_rd_kafka_conf_s, cErr unsafe.Pointer) error {
	for k, v := range values {
		strVal, err := stringify(v)
		if err != nil {
			return err
		}
		if C.rd_kafka_conf_set(kafkaConf, C.CString(k), C.CString(strVal), (*C.char)(cErr), 128) != C.RD_KAFKA_CONF_OK {
			C.rd_kafka_conf_destroy(kafkaConf)
			goErrString := C.GoString((*C.char)(cErr))
			C.free(cErr)
			return fmt.Errorf("could not set %s: %+v", k, goErrString)
		}
	}

	return nil
}

func (conf *ConsumerConfiguration) setup() (*C.struct_rd_kafka_conf_s, error) {
	allBrokers := strings.Join(conf.Brokers, ",")
	cErr := C.malloc(C.size_t(128))

	cconf := C.rd_kafka_conf_new()

	rdKafkaKeyMap := map[string]interface{}{
		"bootstrap.servers": allBrokers,
		"group.id":          conf.GroupID,
		// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
		"socket.nagle.disable": true,
	}

	err := setCConf(rdKafkaKeyMap, cconf, cErr)
	if err != nil {
		return nil, err
	}

	err = setCConf(conf.LibKafkaConf, cconf, cErr)
	if err != nil {
		return nil, err
	}

	// Go being Go https://github.com/golang/go/issues/19835
	C.rd_kafka_conf_set_rebalance_cb(cconf, (*[0]byte)(C.rebalanceCgo))

	return cconf, nil
}

func (conf *ProducerConfiguration) setup() (*C.struct_rd_kafka_conf_s, error) {
	allBrokers := strings.Join(conf.Brokers, ",")
	cErr := C.malloc(C.size_t(128))

	cconf := C.rd_kafka_conf_new()

	rdKafkaKeyMap := map[string]interface{}{
		"bootstrap.servers": allBrokers,
		// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
		"socket.nagle.disable": true,
	}

	err := setCConf(rdKafkaKeyMap, cconf, cErr)
	if err != nil {
		return nil, err
	}

	err = setCConf(conf.LibKafkaConf, cconf, cErr)
	if err != nil {
		return nil, err
	}

	// * The callback is only triggered from rd_kafka_poll() and
	// * rd_kafka_flush().
	// Go being Go https://github.com/golang/go/issues/19835
	C.rd_kafka_conf_set_dr_msg_cb(cconf, (*[0]byte)(C.drCbCgo))

	return cconf, nil
}

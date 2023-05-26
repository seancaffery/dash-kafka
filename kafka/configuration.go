package kafka

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>

extern void rebalanceCgo(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque);
extern void drCbCgo(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
extern int statsCb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque);
*/
import "C"
import (
	"fmt"
	"runtime/cgo"
	"strings"
	"unsafe"
)

type StatisticsCallback func(statsJSON string)

type SharedConfiguration struct {
	Brokers []string
	// Configuration passed directly to librdkafka
	// Provides an escape hatch for configuration not directly exposed. Prefer to add explicit configuration.
	LibKafkaConf       map[string]interface{}
	StatisticsInterval int
	StatisticsCallback StatisticsCallback
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

func (conf *ConsumerConfiguration) setup(cMap map[string]interface{}) (*C.struct_rd_kafka_conf_s, error) {
	allBrokers := strings.Join(conf.Brokers, ",")
	cErr := C.malloc(C.size_t(128))

	cconf := C.rd_kafka_conf_new()

	rdKafkaKeyMap := map[string]interface{}{
		"bootstrap.servers": allBrokers,
		"group.id":          conf.GroupID,
		// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
		"socket.nagle.disable":     true,
		"enable.auto.commit":       false,
		"enable.auto.offset.store": false,
		"statistics.interval.ms":   conf.StatisticsInterval,
		"client.software.name":     "dash-kafka",
		"client.software.version":  "v0.1",
	}

	err := setCConf(rdKafkaKeyMap, cconf, cErr)
	if err != nil {
		return nil, err
	}

	err = setCConf(conf.LibKafkaConf, cconf, cErr)
	if err != nil {
		return nil, err
	}

	C.rd_kafka_conf_set_opaque(cconf, unsafe.Pointer(cgo.NewHandle(cMap)))

	// Go being Go https://github.com/golang/go/issues/19835
	C.rd_kafka_conf_set_rebalance_cb(cconf, (*[0]byte)(C.rebalanceCgo))

	if conf.StatisticsInterval > 0 && conf.StatisticsCallback != nil {
		C.rd_kafka_conf_set_stats_cb(cconf, (*[0]byte)(C.statsCb))
	}

	return cconf, nil
}

func (conf *ProducerConfiguration) setup(cMap map[string]interface{}) (*C.struct_rd_kafka_conf_s, error) {
	allBrokers := strings.Join(conf.Brokers, ",")
	cErr := C.malloc(C.size_t(128))

	cconf := C.rd_kafka_conf_new()

	rdKafkaKeyMap := map[string]interface{}{
		"bootstrap.servers": allBrokers,
		// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets.
		"socket.nagle.disable":    true,
		"statistics.interval.ms":  conf.StatisticsInterval,
		"client.software.name":    "dash-kafka",
		"client.software.version": "v0.1",
	}

	err := setCConf(rdKafkaKeyMap, cconf, cErr)
	if err != nil {
		return nil, err
	}

	err = setCConf(conf.LibKafkaConf, cconf, cErr)
	if err != nil {
		return nil, err
	}

	C.rd_kafka_conf_set_opaque(cconf, unsafe.Pointer(cgo.NewHandle(cMap)))

	// * The callback is only triggered from rd_kafka_poll() and
	// * rd_kafka_flush().
	// Go being Go https://github.com/golang/go/issues/19835
	C.rd_kafka_conf_set_dr_msg_cb(cconf, (*[0]byte)(C.drCbCgo))

	if conf.StatisticsInterval > 0 && conf.StatisticsCallback != nil {
		C.rd_kafka_conf_set_stats_cb(cconf, (*[0]byte)(C.statsCb))
	}

	return cconf, nil
}

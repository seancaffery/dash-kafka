package kafka

/*
#include <librdkafka/rdkafka.h>
extern void goRebalance(rd_kafka_t*, rd_kafka_resp_err_t, rd_kafka_topic_partition_list_t*, void*);

void rebalanceCgo(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque) {
  goRebalance(rk, err, partitions, opaque);
}
*/
import "C"

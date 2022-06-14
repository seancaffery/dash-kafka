package kafka

/*
#include <librdkafka/rdkafka.h>
extern void goRebalance(rd_kafka_t*, rd_kafka_resp_err_t, rd_kafka_topic_partition_list_t*, void*);

void rebalanceCgo(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque) {
  goRebalance(rk, err, partitions, opaque);
}

extern void goDrCb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);

void drCbCgo(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
  goDrCb(rk, rkmessage, opaque);
}

extern int goStatsCb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque);
int statsCb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
  return goStatsCb(rk, json, json_len, opaque);
}
*/
import "C"

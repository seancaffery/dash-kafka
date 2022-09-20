# dash kafka - a simple Go Kafka client

A Go Kafka client backed by [librdkafka](https://github.com/edenhill/librdkafka).

This is a project for me to learn and experiment with Go features that I don't necessarily have the ability to do through my regular work.

Features are based on my experience using various Kafka clients in Ruby, Java, Scala and Go. The focus is on creating a consumer and producer that integrates cleanly into an application, and takes care of most of the complexity of adding a Kafka client to an application.

### But, like, why?

[Just for fun. No, really](https://justforfunnoreally.dev/)

### Goal features

- [ ] Simple, idiomatic Go consumer and producer interface
- [ ] Per-patition consumer
- [ ] Extensibility through 'middleware style' interface
- [ ] In-partition concurrent message processing

### Not goals

- Be a fully feautred Kafka client e.g. it's very unlikely that this will ever implement admin fuctions
- Be a clone of confluent-kafka-go

### Should you use this in production?

Almost certainly not.

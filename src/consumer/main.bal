import ballerina/kafka;
import ballerina/log;

kafka:ConsumerConfiguration consumerConfiguration = {
    bootstrapServers: "localhost:9092",
    groupId: "avro-consumer-group",
    topics: ["add-person-with-account"],
    valueDeserializerType: kafka:DES_AVRO,
    schemaRegistryUrl: "http://localhost:8081/"
};

listener kafka:Consumer consumer = new(consumerConfiguration);

service KafkaService on consumer {
    resource function onMessage(kafka:Consumer consumer, kafka:ConsumerRecord[] records) {
        foreach var kafkaRecord in records {
            anydata value = kafkaRecord.value;
            if (value is kafka:AvroGenericRecord) {
                log:printInfo(value.toString());
            } else {
                log:printError("Invalid record type received.");
            }
        }
    }
}

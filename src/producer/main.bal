import ballerina/io;
import ballerina/kafka;

public type Person record {
    string name;
    int age;
    Account account;
};

public type Account record {
    int accountNumber;
    float balance;
};

string schema = "{\"type\" : \"record\"," +
                  "\"namespace\" : \"Thisaru\"," +
                  "\"name\" : \"person\"," +
                  "\"fields\" : [" + 
                    "{ \"name\" : \"name\", \"type\" : \"string\" }," +
                    "{ \"name\" : \"age\", \"type\" : \"int\" }," +
                    "{ \"name\" : \"account\"," +
                        "\"type\" : {" +
                            "\"type\" : \"record\"," +
                            "\"name\" : \"account\"," +
                            "\"fields\" : [" +
                                "{ \"name\" : \"accountNumber\", \"type\" : \"int\" }," +
                                "{ \"name\" : \"balance\", \"type\" : \"double\" }" +
                            "]}" +
                        "}" +
                  "]}";

kafka:ProducerConfiguration producerConfiguration = {
    bootstrapServers: "localhost:9092",
    valueSerializerType: kafka:SER_AVRO,
    schemaRegistryUrl: "http://localhost:8081"
};

public function main() {
    Account account = {
        accountNumber: 19900515,
        balance: 123.23
    };
    Person person = {
        name: "Lahiru Perera",
        age: 28,
        account: account
    };

    kafka:AvroRecord avroRecord = {
        schemaString: schema,
        dataRecord: person
    };

    kafka:Producer kafkaProducer = new(producerConfiguration);
    var result = kafkaProducer->send(avroRecord, "add-person-with-acount");
    if (result is kafka:ProducerError) {
        io:println(result);
    } else {
        io:println("Successfuly sent");
    }
}

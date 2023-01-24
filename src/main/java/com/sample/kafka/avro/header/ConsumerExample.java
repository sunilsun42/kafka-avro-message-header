package com.sample.kafka.avro.header;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ConsumerExample {

    private static final String TOPIC = "header-test";
    private static final Properties props = new Properties();

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException, ClassNotFoundException {


        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-payments-1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        try (final KafkaConsumer<String,Student> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                final ConsumerRecords<String, Student> records = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<String, Student> record : records) {
                    final String key = record.key();
                    final Student value = record.value();
                    for (Header header : record.headers()) {
                        if(header.key().equalsIgnoreCase("SampleHeader")){
                            ByteArrayInputStream bos = new ByteArrayInputStream(header.value());
                            ObjectInputStream oos = new ObjectInputStream(bos);
                            List<String> impactedFields = (List<String>)oos.readObject();
                            System.out.println("header key " + header.key() );
                            impactedFields.forEach(impactedField -> System.out.println(impactedField));

                        }
                    }
                    System.out.printf("key = %s, value = %s%n", key, value);
                }
            }

        }
    }

}

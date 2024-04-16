package com.helloworld.kafka.pruebasmiguel.consumers;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CamionesConsumerCommitAsync {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "km_posicion";
    private static final String GROUP_ID = "camionesConsumer";
    private static final int NUM_CONSUMERS = 3;

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(NUM_CONSUMERS);
        List<KafkaConsumer<String, GenericRecord>> consumers = new ArrayList<>();

        try {
            for (int i = 0; i < NUM_CONSUMERS; i++) {
                final int numConsumer = i;
                KafkaConsumer<String, GenericRecord> consumer = createConsumer();
                consumers.add(consumer);
                executor.submit(() -> runConsumer(consumer, numConsumer));
            }

            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }
    }

    private static KafkaConsumer<String, GenericRecord> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put("enable.auto.commit", "false");

        return new KafkaConsumer<>(props);
    }

    private static void runConsumer(KafkaConsumer<String, GenericRecord> consumer, int numConsumer) {
        try {
            consumer.subscribe(Collections.singletonList(TOPIC));
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    processRecords(records, consumer, numConsumer);
                    consumer.commitAsync();
                    System.out.println("Commit realizado por consumidor " + numConsumer);
                }
            }
        } catch (WakeupException e) {
            // Ignored for shutdown
        } finally {
            consumer.close();
        }
    }

    private static void processRecords(ConsumerRecords<String, GenericRecord> records, KafkaConsumer<String, GenericRecord> consumer, int numConsumer) {
        Collection<TopicPartition> partitions = new ArrayList<>();
        for (ConsumerRecord<String, GenericRecord> record : records) {
            System.out.printf("Consumer = %d, offset = %d, partition = %d, key = %s, value = %s%n",
                    numConsumer, record.offset(), record.partition(), record.key(), record.value());
            partitions.add(new TopicPartition(record.topic(), record.partition()));
        }
        consumer.commitAsync(new HashMap<>(), (offsets, exception) -> {
            if (exception != null) {
                System.err.println("Error al rxealizar commit asíncrono: " + exception.getMessage());
            } else {
                System.out.println("Commit asíncrono exitoso para consumidor " + numConsumer);
            }
        });
    }
}


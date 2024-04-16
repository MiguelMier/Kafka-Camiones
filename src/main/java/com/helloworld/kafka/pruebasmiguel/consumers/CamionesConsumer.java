package com.helloworld.kafka.pruebasmiguel.consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;


import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CamionesConsumer {
	
	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(final String[] args) throws Exception {
        final String topic = "km_posicion";
        
        String configFile = "./config/default.properties";
        log.info("Default config file "+configFile);
        if (args.length > 0){
        	configFile = args[0];
            log.info("Customized config file "+configFile);
        }

        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // Define grupo y offset
    	final String consumerGroup = "camionesConsumer";
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println(
                            String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));
                }
            }
        }
    }

}
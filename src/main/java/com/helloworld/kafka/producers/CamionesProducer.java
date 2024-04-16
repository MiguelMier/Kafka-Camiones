package com.helloworld.kafka.producers;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CamionesProducer {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(final String[] args) throws IOException, ExecutionException, InterruptedException {

        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        // Configurar el tiempo de espera y el tamaño del batch
        props.put("linger.ms", 100); // tiempo de espera de 100 ms
        props.put("batch.size", 1000); // tamaño del batch de 1000 bytes

        final String topic = "km_posicion";

        final Producer<String, String> producer = new KafkaProducer<>(props);

        final Random rnd = new Random();
        final int numCamiones = 5;
        final int numMediciones = 10;
        final double avancePorMedicion = 1.3; // Avance en kilómetros por cada medición

        // Estructura de datos para mantener la posición de los camiones
        int[] posiciones = new int[numCamiones];
        for (int i = 0; i < numCamiones; i++) {
            posiciones[i] = rnd.nextInt(11) + 10; // Kilómetro inicial aleatorio entre 10 y 20
        }

        // Simulación de avance de los camiones
        for (int i = 0; i < numMediciones; i++) {
            for (int camionId = 0; camionId < numCamiones; camionId++) {
                final int finalCamionId = camionId; // Variable final para utilizar en la lambda
                posiciones[finalCamionId] += avancePorMedicion;

                // Enviar la posición actual del camión al topic
                producer.send(new ProducerRecord<>(topic, Integer.toString(finalCamionId), Double.toString(posiciones[finalCamionId])),
                        (metadata, exception) -> getFutureRecordMetadata(Integer.toString(finalCamionId), Double.toString(posiciones[finalCamionId]), metadata, exception));
            }
            // Pausa de tiempo simulando el intervalo entre mediciones
            Thread.sleep(1000); // Pausa de 1 segundo entre mediciones
        }

        log.info("{} mediciones de posición de camiones enviadas al topic {}", numMediciones * numCamiones, topic);

        producer.close();
    }

    public static void getFutureRecordMetadata(String camionId, String posicion, RecordMetadata metadata, Exception exception) {
        if (exception != null)
            exception.printStackTrace();
        else
            log.info("Produced event to topic {}: camion={} posicion={} partition={}",
                    metadata.topic(), camionId, posicion, metadata.partition());
    }
}
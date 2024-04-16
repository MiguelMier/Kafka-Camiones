package com.helloworld.kafka.pruebasmiguel.producers;

import java.io.IOException;
//import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;


public class CamionesProducer {
    
    //private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(final String[] args) throws IOException, ExecutionException, InterruptedException {

        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        // Configurar el tiempo de espera y el tamaño del batch --> en este ejemplo NO
        //props.put("linger.ms", 100); // tiempo de espera de 100 ms
        //props.put("batch.size", 1000); // tamaño del batch de 1000 bytes

        final String topic = "km_posicion";

        // campos: si esta en movimiento o parado (velocidad --> int), posicion gps (latitud y longitud -> string), 
        // matricula (string), timestamp del momento
        String keySchemaString = """
            {
                "namespace": "test",
                "type": "record",
                "name": "key",
                "fields": [
                    {
                        "name": "matricula",
                        "type": "string"
                    }
                ]
            }
        """;

        String valueSchemaString = """
            {
                "namespace": "test",
                "type": "record",
                "name": "value",
                "fields": [
                    {
                        "name": "km",
                        "type": "float"
                    },
                    {
                        "name": "matricula",
                        "type": "string"
                    },
                    {
                        "name": "velocidad",
                        "type": "int"
                    },
                    {
                        "name": "timestamp_medicion",
                        "type": "float"
                    }
                ]
            }
        """;
        Schema keySchema = new Schema.Parser().parse(keySchemaString);
        Schema valueSchema = new Schema.Parser().parse(valueSchemaString);

        final Producer<String, String> producer = new KafkaProducer<>(props);

        final Random rnd = new Random();
        final int numCamiones = 5;
        final int numMediciones = 10;
        final double avancePorMedicion = 1.3; // Avance en kilómetros por cada medición

        // Array para saber la posicion de los camiones
        int[] posiciones = new int[numCamiones];
        for (int i = 0; i < numCamiones; i++) {
            posiciones[i] = rnd.nextInt(11) + 10; // Kilómetro inicial aleatorio entre 10 y 20
        }

        // Simulación de avance de los camiones
        for (int i = 0; i < numMediciones; i++) {
            for (int camionId = 0; camionId < numCamiones; camionId++) {
                final int finalCamionId = camionId; 
                posiciones[finalCamionId] += avancePorMedicion;

                final String camionMandar = "Camion " + Integer.toString(finalCamionId);
                final String posicionMandar = "Posicion: " + Double.toString(posiciones[finalCamionId]);

                // Enviar la posición actual del camión al topic
                producer.send(new ProducerRecord<>(topic, camionMandar, posicionMandar),
                        (event, exception) -> getFutureRecordMetadata(camionMandar, posicionMandar, event, exception));
            }
            Thread.sleep(1000); 
        }

        System.out.printf("{} mediciones de posición de camiones enviadas al topic {}", numMediciones * numCamiones, topic);
        
        producer.flush(); // fuerza al productor a enviar todos los registros pendientes de enviar a los brokers
        producer.close();
    }

    public static void getFutureRecordMetadata(String camionId, String posicion, RecordMetadata metadata, Exception exception) {
        if (exception != null)
            exception.printStackTrace();
        else
            System.out.printf("Produced event to topic %s: camion=%s posicion=%s partition=%d%n",
                    metadata.topic(), camionId, posicion, metadata.partition());
    }
}
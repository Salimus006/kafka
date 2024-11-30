package org.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Producer {
    private final String KAFKA_BROKER_URL;
    private final String TOPIC_NAME;
    private final String PRODUCER_ID;


    public Producer(String kafkaBrokerUrl, String topicName, String producerId) {
        KAFKA_BROKER_URL = kafkaBrokerUrl;
        TOPIC_NAME = topicName;
        PRODUCER_ID = producerId;
    }

    public void start() {

        Random random = new Random();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {

            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(initProperties());
            // en mode bloquant
            //Future<RecordMetadata> result = producer.send(new ProducerRecord<>(TOPIC_NAME, "key1","value1"));

            String key = String.valueOf(random.nextInt(10000));
            String val = String.valueOf(random.nextDouble()*9999);
            // en mode assunchrone
            producer.send(new ProducerRecord<>(TOPIC_NAME, key, val), (recordMetadata, e) -> {
                if(e == null) {
                    System.out.println("message key => " + key + " envoyé avec succès sur le topic => " + recordMetadata.topic() + " partition => " + recordMetadata.topic() + " offset => " + recordMetadata.offset());
                } else {
                    System.out.println("Erreur lors de l'envoi du message key => " + key + ". Détail => " + e.getMessage());
                }
            });


        }, 1000, 100, TimeUnit.MILLISECONDS);

    }

    private Properties initProperties() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_URL);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return props;
    }
}

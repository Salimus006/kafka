package org.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Producer {

    public static void main(String[] args) {
        new Producer().start();
    }

    private void start() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-to-stream-test");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        //
        List<Character> characters = new ArrayList<>();
        for (char c = 'A'; c < 'Z'; c++) {
            characters.add(c);
        }

        Random random = new Random();
        String outTopic = "stream_topic_test";
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            // chaque message est une ligne de caractères séparés par des espaces
            // exemple : A B Z E
            String message = "";
            for (int i = 0; i < 10; i++) {
                message+=" " + characters.get(random.nextInt(characters.size()));
                String finalMessage = message;
                kafkaProducer.send(new ProducerRecord<>(outTopic, null, message), ((recordMetadata, e) -> {
                    if (e == null) {
                        System.out.println("Message [" + finalMessage + "] envoyé avec succès sur le topic ["+ outTopic+"] " +
                                "partition [" +recordMetadata.partition()+"], offset [" + recordMetadata.offset()+"]");
                    }
                    else {
                        System.out.println("Erreur d'envoi sur le topic [" + outTopic+ "]. Détail: " + e.getMessage());
                    }
                }));
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS); // un thread chaque seconde

    }
}

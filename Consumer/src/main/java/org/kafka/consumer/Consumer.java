package org.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Consumer {

    private final String KAFKA_BROKER_URL;
    private final String TOPIC_NAME;
    private final String CONSUMER_GROUP;

    public Consumer(String kafkaBrokerUrl, String topicName, String consumerGroup) {
        KAFKA_BROKER_URL = kafkaBrokerUrl;
        TOPIC_NAME = topicName;
        CONSUMER_GROUP = consumerGroup;
    }

    public void start() {
        System.out.println("Démarrage du listener avec le topic " + TOPIC_NAME + " groupe " + CONSUMER_GROUP);
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(initProperties());
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        // chaque seconde je fais un pull pour récupérer les message (En créant un thread avec un timer)
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            System.out.println("-----------------------------------------------");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> System.out.println("topic => " + TOPIC_NAME +
                    " partition => "+ record.partition() + " offset => " + record.offset() + " key " + record.key() +" msg => " + record.value()));
        }, 2000, 2000, TimeUnit.MILLISECONDS);
    }

    private Properties initProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}

package org.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        new Consumer().start();
    }

    private void start() {
        // config du stream
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer-test");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // par défaut
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // par défaut
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // 1000 ms == 1s

        // création du stream
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // on peut surcharger la KEY_SERDE et VALUR_SERDE
        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();
        // construction du KStream (subscribe au topic en spécifiant la class de serialization de la clé/value)
        KStream<String, String> kStream = streamsBuilder.stream("stream_topic_test", Consumed.with(stringSerde, stringSerde));

        // traitement de la data (on fait que afficher les données qu'on recoit)
        /*
        kStream.foreach((key, value) -> {
            System.out.println("message key ==> [" + key +"] value ==> [" + value +"]");
        });
         */

        // je veux juste split chaque ligne
        /*
        kStream.flatMapValues(value -> Arrays.asList(value.split(" "))).foreach((k,v) -> {
            System.out.println("k==> " +k+ " / value ==> " + v);
        });
         */

        KTable<String, Long> kTableResult = kStream.flatMapValues(textValue -> Arrays.asList(textValue.split(" ")))
                .map((k,v) -> new KeyValue<>(k,v.toLowerCase())) // je met la valeur en lowerCase
                .filter((k,v) -> "a".equals(v) || "b".equals(v)) // je filtre pour garder que 'a' et 'b'
                        .groupBy((k,v) -> v)
                                .count(Materialized.as("count-analytics")); // on sauvegarde le résultat dans une vue matérialisée
                //.foreach((k,v) -> System.out.println("k==> " +k+ " / value ==> " + v));

        // je transforme l'objet KTable en KStream
        KStream<String, Long> kStreamResult = kTableResult.toStream();
        // j'envoie vers un autre topic en sortie
        kStreamResult.to("res_stream_topic", Produced.with(stringSerde, longSerde));
        // Le résultat qui sera obtenue (cumul du nombre de 'a' et 'b' recus depuis le début)
        /*
        a	1614
        b	1718
        a	1619
        b	1730
        a	1620
        a	1624
        b	1740
        a	1629
        b	1748
        a	1630
        b	1750
        a	1633
        b	1760
         */
        // si on veut obtenir le même résultat mais sur une fenêtre des 5 dernières secondes
        KTable<Windowed<String>, Long> kTableParFenetre = kStream.flatMapValues(textValue -> Arrays.asList(textValue.split(" ")))
                .map((k,v) -> new KeyValue<>(k,v.toLowerCase())) // je met la valeur en lowerCase
                .filter((k,v) -> "a".equals(v) || "b".equals(v)) // je filtre pour garder que 'a' et 'b'
                .groupBy((k,v) -> v)
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .count(Materialized.as("count-analytics"));

        kTableParFenetre.toStream().map((k,v) -> new KeyValue<>(k.key(), v)).to("un_autre_test");



        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        kafkaStreams.start();
    }
}

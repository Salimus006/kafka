package org.kafka;

import org.kafka.consumer.Consumer;

public class Main {
    public static void main(String[] args) {
        Consumer consumer = new Consumer("localhost:9092", "test", "test-group-1");
        consumer.start();
    }
}
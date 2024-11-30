package org.kafka;

public class Main {
    public static void main(String[] args) {
        Producer producer = new Producer("localhost:9092", "test", "producer-1");
        producer.start();
    }
}
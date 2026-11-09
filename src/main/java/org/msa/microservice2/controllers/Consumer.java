package org.msa.microservice2.controllers;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ConcurrentLinkedQueue;

@RestController
public class Consumer {

    // Kafka's listener adds messages here; HTTP requests remove them from the front.
    // The concurrent queue safely coordinates those two threads and preserves FIFO order.
    private final ConcurrentLinkedQueue<String> messages = new ConcurrentLinkedQueue<>();

    @GetMapping("/consume/message")
    public String consume() {
        // poll() removes and returns the oldest queued message, or null if the queue is empty.
        String message = messages.poll();
        return message == null ? "No messages available" : message;
    }

    @KafkaListener(topics = "messages", groupId = "microservice2")
    public void onMessage(String message) {
        // Kafka delivers records in topic order; adding each one keeps that order for HTTP reads.
        messages.add(message);
        System.out.println("Received Kafka message: " + message);
    }

}

package com.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Aplicacion Spring Boot para el tutorial de Kafka.
 * Demuestra como funcionan topics, particiones y offsets.
 */
@SpringBootApplication
public class KafkaTutorialApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTutorialApplication.class, args);
    }
}

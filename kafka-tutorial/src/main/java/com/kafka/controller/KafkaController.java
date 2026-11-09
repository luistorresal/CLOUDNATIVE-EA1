package com.kafka.controller;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controlador REST que actua como PRODUCTOR: envia mensajes a Kafka.
 *
 * Demuestra dos conceptos:
 *  - Sin key: Kafka reparte los mensajes entre las particiones 0, 1, 2.
 *  - Con key: Kafka calcula hash(key) y todos los mensajes con esa key
 *    van a la MISMA particion, conservando su orden.
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    public static final String TOPIC = "tutorial-particiones";

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Ejemplos de uso:
     *  SIN KEY: GET http://localhost:8888/api/kafka/send?message=Mensaje1
     *  CON KEY: GET http://localhost:8888/api/kafka/send?message=Paso1-Creada&key=orden-555
     */
    @GetMapping("/send")
    public String sendMessage(@RequestParam String message,
                              @RequestParam(required = false) String key) {

        if (key != null && !key.isEmpty()) {
            // Con key: misma key -> misma particion -> orden garantizado
            kafkaTemplate.send(TOPIC, key, message);
            return "Mensaje enviado al topic '" + TOPIC + "' con KEY: " + key
                    + " | MENSAJE: " + message;
        }

        // Sin key: Kafka elige la particion automaticamente
        kafkaTemplate.send(TOPIC, message);
        return "Mensaje enviado al topic '" + TOPIC
                + "' (sin key - distribucion automatica) | MENSAJE: " + message;
    }

    /** Endpoint de verificacion. */
    @GetMapping("/health")
    public String health() {
        return "Aplicación de Kafka Tutorial está corriendo en puerto 8888";
    }
}

package com.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * CONSUMIDOR de Kafka que escucha el topic 'tutorial-particiones'.
 *
 * Muestra en tiempo real:
 *  - Particion: en cual de las 3 particiones llego el mensaje
 *  - Offset:    posicion secuencial dentro de ESA particion
 *  - Key:       la clave del mensaje (null si no se envio)
 *  - Mensaje:   el contenido
 */
@Service
public class KafkaConsumer {

    @KafkaListener(topics = "tutorial-particiones", groupId = "mi-grupo-educativo")
    public void listen(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            // required = false: los mensajes sin key no traen este header
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        System.out.println("\n" + "=".repeat(60));
        System.out.println(" MENSAJE RECIBIDO");
        System.out.printf("   Partición: %d%n", partition);
        System.out.printf("   Offset:    %d%n", offset);
        System.out.printf("   Key:       %s%n", key != null ? key : "(sin key)");
        System.out.printf("   Mensaje:   %s%n", message);
        System.out.println("=".repeat(60) + "\n");
    }
}

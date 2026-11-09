package com.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafka.controller.KafkaController;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Verifica con un Kafka embebido (sin Docker) la regla central de la guia:
 * mensajes con la MISMA key van a la MISMA particion, con offsets consecutivos.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = KafkaController.TOPIC)
class ParticionesTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void mismaKeyVaSiempreALaMismaParticionYEnOrden() throws Exception {
        List<RecordMetadata> enviados = new ArrayList<>();
        for (String paso : List.of("Paso1-Creada", "Paso2-Pagada", "Paso3-Enviada")) {
            enviados.add(kafkaTemplate.send(KafkaController.TOPIC, "orden-555", paso)
                    .get().getRecordMetadata());
        }

        int particion = enviados.get(0).partition();
        long primerOffset = enviados.get(0).offset();

        for (int i = 0; i < enviados.size(); i++) {
            assertThat(enviados.get(i).partition()).isEqualTo(particion);
            assertThat(enviados.get(i).offset()).isEqualTo(primerOffset + i);
        }
    }

    @Test
    void sinKeyElMensajeLlegaAUnaParticionValida() throws Exception {
        RecordMetadata meta = kafkaTemplate.send(KafkaController.TOPIC, "Mensaje1")
                .get().getRecordMetadata();

        assertThat(meta.partition()).isBetween(0, 2);
    }
}

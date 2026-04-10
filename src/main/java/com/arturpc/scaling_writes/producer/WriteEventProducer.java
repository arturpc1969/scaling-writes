package com.arturpc.scaling_writes.producer;

import com.arturpc.scaling_writes.config.KafkaConfig;
import com.arturpc.scaling_writes.domain.WriteEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Publicador de eventos de escrita no tópico Kafka.
 *
 * Estratégia de particionamento:
 *   A mensagem é publicada com chave explícita = (shardKey % 3).
 *   O Kafka usa essa chave para determinar a partition de destino:
 *     - chave 0 → partition 0 → Worker-0 → shard_id=0
 *     - chave 1 → partition 1 → Worker-1 → shard_id=1
 *     - chave 2 → partition 2 → Worker-2 → shard_id=2
 *
 *   Isso garante que eventos do mesmo shard sempre vão para o mesmo worker,
 *   mantendo a coerência do batch e evitando condições de corrida.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WriteEventProducer {

    private final KafkaTemplate<Integer, Object> kafkaTemplate;

    /**
     * Publica um WriteEvent no tópico "write-events" com partition key = shardKey % 3.
     *
     * @param shardKey chave de sharding fornecida pelo cliente (qualquer inteiro)
     * @param payload  conteúdo do evento
     */
    public void publish(int shardKey, String payload) {
        int partitionKey = Math.abs(shardKey) % KafkaConfig.NUM_PARTITIONS;

        WriteEvent event = WriteEvent.builder()
                .shardKey(shardKey)
                .payload(payload)
                .timestamp(Instant.now())
                .build();

        CompletableFuture<SendResult<Integer, Object>> future =
                kafkaTemplate.send(KafkaConfig.TOPIC_WRITE_EVENTS, partitionKey, partitionKey, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[PRODUCER] Falha ao publicar evento (shard={}, partition={}): {}",
                        shardKey, partitionKey, ex.getMessage());
            } else {
                log.info("[PRODUCER] Evento publicado | shard={} → partition={} | offset={}",
                        shardKey, partitionKey,
                        result.getRecordMetadata().offset());
            }
        });
    }
}

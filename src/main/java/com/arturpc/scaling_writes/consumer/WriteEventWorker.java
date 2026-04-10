package com.arturpc.scaling_writes.consumer;

import com.arturpc.scaling_writes.domain.WriteEvent;
import com.arturpc.scaling_writes.domain.WriteEventEntity;
import com.arturpc.scaling_writes.repository.WriteEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Worker que consome eventos do Kafka e os persiste em batch no Cassandra.
 *
 * Funcionamento:
 *   1. @KafkaListener com concurrency=3 cria 3 threads consumers, uma por partition.
 *      Cada thread é associada automaticamente a uma partition (0, 1 ou 2).
 *
 *   2. Cada mensagem recebida é adicionada a um buffer por shard (CopyOnWriteArrayList).
 *      O shard_id é derivado da partition do Kafka (0, 1 ou 2), mantendo a coerência.
 *
 *   3. A cada N segundos (@Scheduled), o método flushAllBuffers() drena os buffers
 *      e executa um batch write (saveAll) no Cassandra para cada shard.
 *
 * Por que usar buffer por shard e não um único buffer global?
 *   - Cada partition/worker processa eventos de um shardKey diferente.
 *   - Manter buffers separados evita contenção e torna o flush mais eficiente:
 *     cada saveAll() vai direto para a partition key correta no Cassandra.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WriteEventWorker {

    private final WriteEventRepository repository;

    /**
     * Buffers indexados por shard_id (0, 1, 2).
     * CopyOnWriteArrayList garante thread-safety entre o consumer thread e o scheduler thread.
     */
    private final Map<Integer, CopyOnWriteArrayList<WriteEvent>> buffers = new ConcurrentHashMap<>(Map.of(
            0, new CopyOnWriteArrayList<>(),
            1, new CopyOnWriteArrayList<>(),
            2, new CopyOnWriteArrayList<>()
    ));

    /**
     * Consome mensagens do tópico "write-events".
     *
     * concurrency = "3": Spring Kafka cria 3 threads consumers, cada uma fixada em uma partition.
     *   - Consumer 0 → partition 0 → shard_id 0
     *   - Consumer 1 → partition 1 → shard_id 1
     *   - Consumer 2 → partition 2 → shard_id 2
     *
     * @param event     o WriteEvent deserializado do Kafka
     * @param partition a partition de origem (equivale ao shard_id)
     */
    @KafkaListener(
            topics = "#{T(com.arturpc.scaling_writes.config.KafkaConfig).TOPIC_WRITE_EVENTS}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "3"
    )
    public void onMessage(
            @Payload WriteEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition
    ) {
        int shardId = partition; // partition e shard_id são equivalentes nesta simulação
        buffers.get(shardId).add(event);

        log.debug("[WORKER-{}] Evento recebido | buffer_size={} | payload={}",
                shardId, buffers.get(shardId).size(), event.getPayload());
    }

    /**
     * Flush periódico de todos os buffers para o Cassandra.
     *
     * Executado a cada N segundos (configurado em app.batch.flush-interval-ms).
     * Para cada shard com eventos pendentes:
     *   1. Drena o buffer atomicamente (swap por lista vazia)
     *   2. Converte os eventos em entidades Cassandra
     *   3. Executa saveAll() — batch write na partition key correspondente
     */
    @Scheduled(fixedDelayString = "${app.batch.flush-interval-ms}")
    public void flushAllBuffers() {
        for (int shardId = 0; shardId < 3; shardId++) {
            flushBuffer(shardId);
        }
    }

    private void flushBuffer(int shardId) {
        CopyOnWriteArrayList<WriteEvent> buffer = buffers.get(shardId);

        if (buffer.isEmpty()) {
            return;
        }

        // Drena o buffer atomicamente: copia e limpa
        List<WriteEvent> snapshot = new ArrayList<>(buffer);
        buffer.removeAll(snapshot);

        Instant writtenAt = Instant.now();

        List<WriteEventEntity> entities = snapshot.stream()
                .map(event -> WriteEventEntity.builder()
                        .shardId(shardId)
                        .eventId(UUID.randomUUID())
                        .payload(event.getPayload())
                        .createdAt(event.getTimestamp())
                        .writtenAt(writtenAt)
                        .build())
                .toList();

        repository.saveAll(entities);

        log.info("[WORKER-{}] Batch escrito no Cassandra | {} eventos | shard_id={}",
                shardId, entities.size(), shardId);
    }

    /**
     * Retorna o tamanho atual do buffer de cada shard.
     * Usado pelo endpoint GET /stats para mostrar eventos pendentes em memória.
     */
    public Map<String, Integer> getBufferSizes() {
        return Map.of(
                "shard_0_pending", buffers.get(0).size(),
                "shard_1_pending", buffers.get(1).size(),
                "shard_2_pending", buffers.get(2).size()
        );
    }
}

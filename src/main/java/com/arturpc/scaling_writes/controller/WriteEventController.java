package com.arturpc.scaling_writes.controller;

import com.arturpc.scaling_writes.consumer.WriteEventWorker;
import com.arturpc.scaling_writes.producer.WriteEventProducer;
import com.arturpc.scaling_writes.repository.WriteEventRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Endpoint REST para simular o recebimento de requisições de escrita em alto volume.
 *
 * POST /events  → publica um evento no Kafka (producer)
 * GET  /stats   → exibe contagem de eventos por shard no Cassandra + buffer atual
 */
@Slf4j
@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
@Tag(name = "Write Events", description = "Simulação de escrita em alto volume com Kafka + Cassandra")
public class WriteEventController {

    private final WriteEventProducer producer;
    private final WriteEventRepository repository;
    private final WriteEventWorker worker;

    /**
     * Recebe um evento de escrita e o publica no Kafka.
     *
     * O shardKey determina em qual partition/worker o evento será processado.
     * Qualquer inteiro é aceito — o producer calculará (shardKey % 3) internamente.
     *
     * Exemplo: POST /events?shardKey=42&payload=minha-mensagem
     */
    @PostMapping
    @Operation(
            summary = "Publica um evento de escrita",
            description = "Envia o evento ao Kafka. O worker responsável irá agregá-lo " +
                          "em buffer e persistir no Cassandra via batch write periodicamente."
    )
    public ResponseEntity<Map<String, Object>> publishEvent(
            @RequestParam int shardKey,
            @RequestParam String payload
    ) {
        producer.publish(shardKey, payload);

        int partition = Math.abs(shardKey) % 3;
        log.info("[CONTROLLER] Evento aceito | shardKey={} → partition={}", shardKey, partition);

        return ResponseEntity.accepted().body(Map.of(
                "status", "accepted",
                "shardKey", shardKey,
                "targetPartition", partition,
                "targetShard", partition
        ));
    }

    /**
     * Retorna estatísticas de escrita por shard.
     *
     * Mostra:
     *   - Total de eventos persistidos no Cassandra por shard (shard_0_count, shard_1_count, shard_2_count)
     *   - Eventos ainda em buffer aguardando o próximo flush (shard_N_pending)
     */
    @GetMapping("/stats")
    @Operation(
            summary = "Estatísticas de escrita por shard",
            description = "Exibe quantos eventos foram persistidos no Cassandra por shard " +
                          "e quantos ainda estão em buffer aguardando o próximo batch write."
    )
    public ResponseEntity<Map<String, Object>> getStats() {
        long shard0 = repository.countByShardId(0);
        long shard1 = repository.countByShardId(1);
        long shard2 = repository.countByShardId(2);

        Map<String, Integer> pendingBuffers = worker.getBufferSizes();

        return ResponseEntity.ok(Map.of(
                "cassandra_persisted", Map.of(
                        "shard_0", shard0,
                        "shard_1", shard1,
                        "shard_2", shard2,
                        "total", shard0 + shard1 + shard2
                ),
                "in_buffer_pending_flush", pendingBuffers
        ));
    }
}

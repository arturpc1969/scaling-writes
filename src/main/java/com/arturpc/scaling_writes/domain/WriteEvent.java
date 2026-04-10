package com.arturpc.scaling_writes.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * DTO que representa um evento de escrita recebido via HTTP e trafegado pelo Kafka.
 *
 * shardKey: valor inteiro fornecido pelo cliente; o producer usará (shardKey % 3)
 *           para determinar em qual partition do Kafka a mensagem será publicada,
 *           garantindo que cada worker processe apenas eventos do seu shard.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WriteEvent {

    /** Chave de sharding — determina em qual partition/worker o evento será processado. */
    private int shardKey;

    /** Conteúdo arbitrário do evento (simula o payload real de escrita). */
    private String payload;

    /** Momento em que o evento foi criado (preenchido pelo producer). */
    private Instant timestamp;
}

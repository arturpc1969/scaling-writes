package com.arturpc.scaling_writes.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Representa um registro da tabela "write_events" no Cassandra.
 *
 * Schema:
 *   CREATE TABLE write_events (
 *     shard_id   INT,          -- PARTITION KEY: 0, 1 ou 2 (um por worker/shard)
 *     event_id   UUID,         -- CLUSTERING KEY: ordena e garante unicidade no shard
 *     payload    TEXT,
 *     created_at TIMESTAMP,    -- quando o evento foi originado pelo producer
 *     written_at TIMESTAMP,    -- quando foi persistido em batch pelo worker
 *     PRIMARY KEY (shard_id, event_id)
 *   );
 *
 * Por que essa partition key funciona como sharding didático:
 *   O Cassandra usa consistent hashing (MurmurHash3) sobre a partition key para
 *   distribuir os dados entre os nós. Com 3 valores distintos (0, 1, 2) e 3 nós
 *   no cluster, cada nó tende a ser o "coordinator natural" de um shard.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WriteEventEntity {

    /**
     * Partition key — define em qual "shard" (token range) o dado será armazenado.
     * Valores possíveis: 0, 1, 2 (um por worker).
     */
    private int shardId;

    /**
     * Clustering key — ordena e garante unicidade dentro do shard.
     */
    private UUID eventId;

    /** Conteúdo do evento. */
    private String payload;

    /** Timestamp de criação do evento (originado pelo producer). */
    private Instant createdAt;

    /** Timestamp de quando o batch foi escrito no Cassandra (preenchido pelo worker). */
    private Instant writtenAt;
}

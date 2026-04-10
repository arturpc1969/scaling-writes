package com.arturpc.scaling_writes.repository;

import com.arturpc.scaling_writes.domain.WriteEventEntity;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Repositório para persistência de WriteEventEntity no Cassandra.
 *
 * Utiliza o java-driver diretamente (CqlSession + PreparedStatement) pois o
 * Spring Boot 4 não inclui mais Spring Data Cassandra no starter padrão.
 *
 * Ordem de inicialização:
 *   1. CassandraConfig.cassandraSchemaReady() cria keyspace + tabela
 *   2. @DependsOn("cassandraSchemaReady") garante que o bean acima
 *      seja instanciado ANTES deste repository
 *   3. @PostConstruct compila o PreparedStatement — neste ponto
 *      a tabela já existe e a query é válida
 *
 * saveAll() usa UNLOGGED BATCH para enviar múltiplos inserts em uma única
 * request ao coordinator do Cassandra, reduzindo overhead de rede.
 *
 * Por que UNLOGGED BATCH?
 *   - LOGGED BATCH oferece atomicidade mas tem overhead alto (batchlog replicado).
 *   - UNLOGGED BATCH é adequado aqui pois todas as linhas têm a mesma partition
 *     key (shard_id), tornando o batch naturalmente atômico para essa partition.
 */
@Slf4j
@Repository
@DependsOn("cassandraSchemaReady")
public class WriteEventRepository {

    private final CqlSession cqlSession;
    private final String keyspaceName;

    // Inicializado no @PostConstruct, após o schema estar criado
    private PreparedStatement insertStmt;

    public WriteEventRepository(CqlSession cqlSession,
                                @Value("${app.cassandra.keyspace-name}") String keyspaceName) {
        this.cqlSession = cqlSession;
        this.keyspaceName = keyspaceName;
    }

    /**
     * Compila o PreparedStatement após o schema estar criado.
     * O @PostConstruct roda depois de todos os @DependsOn serem resolvidos,
     * garantindo que a tabela write_events já existe no Cassandra.
     */
    @PostConstruct
    public void prepareStatements() {
        this.insertStmt = cqlSession.prepare(
                "INSERT INTO " + keyspaceName + ".write_events " +
                "(shard_id, event_id, payload, created_at, written_at) " +
                "VALUES (?, ?, ?, ?, ?)"
        );
        log.info("[REPOSITORY] PreparedStatement compilado para keyspace '{}'.", keyspaceName);
    }

    /**
     * Persiste uma lista de eventos em batch (UNLOGGED) no Cassandra.
     * Todos os eventos devem ter o mesmo shard_id para batch eficiente.
     *
     * @param entities lista de eventos a persistir
     */
    public void saveAll(List<WriteEventEntity> entities) {
        if (entities.isEmpty()) return;

        List<BoundStatement> statements = entities.stream()
                .map(e -> insertStmt.bind(
                        e.getShardId(),
                        e.getEventId(),
                        e.getPayload(),
                        e.getCreatedAt(),
                        e.getWrittenAt()
                ))
                .toList();

        BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED)
                .addAll(statements);

        cqlSession.execute(batch);
        log.debug("[REPOSITORY] Batch de {} eventos escritos no shard_id={}",
                entities.size(), entities.get(0).getShardId());
    }

    /**
     * Conta o total de eventos armazenados em um determinado shard.
     *
     * @param shardId 0, 1 ou 2
     * @return contagem de eventos nesse shard
     */
    public long countByShardId(int shardId) {
        Row row = cqlSession.execute(
                "SELECT COUNT(*) FROM " + keyspaceName + ".write_events WHERE shard_id = " + shardId
        ).one();
        return row != null ? row.getLong(0) : 0L;
    }
}

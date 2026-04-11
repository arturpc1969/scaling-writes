package com.arturpc.scaling_writes.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.cassandra.autoconfigure.CassandraAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import java.net.InetSocketAddress;

/**
 * Configuração do Cassandra com bootstrap em duas fases:
 *
 * Fase 1 — bootstrapSession (sem keyspace):
 *   Cria uma sessão sem keyspace para executar CREATE KEYSPACE e CREATE TABLE.
 *   Necessário porque o keyspace ainda não existe no primeiro boot.
 *
 * Fase 2 — cassandraSession @Primary (com keyspace):
 *   Cria a sessão definitiva conectada ao keyspace, que será injetada
 *   em todos os outros beans (WriteEventRepository, etc).
 *
 * Por que não usar spring.cassandra.keyspace-name no application.yaml?
 *   O Spring Boot autoconfigure cria o CqlSession com o keyspace configurado.
 *   Se o keyspace não existe, o driver lança InvalidKeyspaceException na startup.
 *   Ao não configurar o keyspace no yaml, o autoconfigure cria uma sessão sem
 *   keyspace. Aqui sobrescrevemos essa sessão com uma que aponta para o keyspace
 *   que acabamos de criar.
 *
 * Schema da tabela write_events:
 *   - PARTITION KEY: shard_id (0, 1, 2) → distribui dados entre nós por consistent hashing
 *   - CLUSTERING KEY: event_id (UUID)   → ordena e garante unicidade dentro do shard
 */
@Slf4j
@Configuration
public class CassandraConfig {

    @Value("${spring.cassandra.contact-points:localhost:9042}")
    private String contactPoints;

    @Value("${spring.cassandra.local-datacenter:dc1}")
    private String localDatacenter;

    @Value("${app.cassandra.keyspace-name:scaling_writes}")
    private String keyspaceName;

    @Value("${app.cassandra.replication-factor:1}")
    private int replicationFactor;

    /**
     * Sessão sem keyspace usada apenas para criar o schema.
     * Fechada automaticamente após a inicialização.
     */
    private CqlSession openBootstrapSession() {
        String[] parts = contactPoints.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9042;

        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(localDatacenter)
                .build();
    }

    /**
     * Bean @Primary que fornece o CqlSession com keyspace para toda a aplicação.
     *
     * Fluxo:
     *   1. Abre sessão bootstrap sem keyspace
     *   2. Cria keyspace + tabela se não existirem
     *   3. Fecha sessão bootstrap
     *   4. Abre e retorna sessão definitiva conectada ao keyspace
     */
    @Bean
    @Primary
    public CqlSession cassandraSession() {
        // Fase 1: bootstrap sem keyspace
        try (CqlSession bootstrap = openBootstrapSession()) {
            log.info("Inicializando schema do Cassandra (keyspace={}, rf={})...", keyspaceName, replicationFactor);

            // O nome do DC no CREATE KEYSPACE deve ser idêntico ao reportado pelos nós
            // via gossip — que é o valor de CASSANDRA_DC no docker-compose.yml.
            // Usar a variável localDatacenter (lida de spring.cassandra.local-datacenter)
            // garante consistência: se o yaml diz "dc1", o keyspace também usa "dc1".
            bootstrap.execute(String.format(
                    "CREATE KEYSPACE IF NOT EXISTS %s " +
                    "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': %d} " +
                    "AND durable_writes = true;",
                    keyspaceName, replicationFactor
            ));
            log.info("Keyspace '{}' verificado/criado (replication-factor={}).", keyspaceName, replicationFactor);

            bootstrap.execute(String.format(
                    "CREATE TABLE IF NOT EXISTS %s.write_events (" +
                    "  shard_id   INT, " +
                    "  event_id   UUID, " +
                    "  payload    TEXT, " +
                    "  created_at TIMESTAMP, " +
                    "  written_at TIMESTAMP, " +
                    "  PRIMARY KEY (shard_id, event_id)" +
                    ");",
                    keyspaceName
            ));
            log.info("Tabela 'write_events' verificada/criada.");
        }

        // Fase 2: sessão definitiva com keyspace
        String[] parts = contactPoints.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9042;

        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(host, port))
                .withLocalDatacenter(localDatacenter)
                .withKeyspace(keyspaceName)
                .build();

        log.info("CqlSession criado com keyspace '{}'.", keyspaceName);
        return session;
    }

    /**
     * Bean sentinela: garante que o WriteEventRepository declare @DependsOn
     * neste nome e seja inicializado APÓS o cassandraSession (que cria o schema).
     */
    @Bean
    public String cassandraSchemaReady(CqlSession cassandraSession) {
        return "schema-ready";
    }
}

# scaling-writes

Simulação didática de um sistema de escrita em alto volume utilizando **Apache Kafka**, **workers com agregação em batch** e **Apache Cassandra** com sharding por partition key.

O objetivo é demonstrar como requisições de escrita em grande volume podem ser distribuídas e processadas de forma eficiente, evitando sobrecarga direta no banco de dados.

---

## Visão geral da arquitetura

```
Cliente HTTP
    │
    │  POST /events?shardKey=N&payload=...
    ▼
┌────────────────────────┐
│  WriteEventController  │  ← REST API (Spring MVC)
└────────────────────────┘
    │
    │  publica mensagem com chave = (shardKey % 3)
    ▼
┌────────────────────────┐
│   WriteEventProducer   │  ← Kafka Producer
└────────────────────────┘
    │
    │  tópico: write-events (3 partitions)
    ▼
┌─────────────┬─────────────┬─────────────┐
│ Partition 0 │ Partition 1 │ Partition 2 │
└─────────────┴─────────────┴─────────────┘
    │              │              │
    ▼              ▼              ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│ Worker 0 │ │ Worker 1 │ │ Worker 2 │  ← WriteEventWorker (concurrency=3)
│ buffer[] │ │ buffer[] │ │ buffer[] │
└──────────┘ └──────────┘ └──────────┘
    │              │              │
    └──────────────┴──────────────┘
                   │
         @Scheduled (a cada N segundos)
         Batch Write via UNLOGGED BATCH
                   │
                   ▼
        ┌────────────────────┐
        │    Cassandra       │
        │  shard_id = 0/1/2  │  ← partition key
        └────────────────────┘
```

### Componentes

| Componente | Tecnologia | Responsabilidade |
|---|---|---|
| REST API | Spring MVC | Recebe requisições de escrita e aceita imediatamente (HTTP 202) |
| Producer | Spring Kafka | Publica eventos no tópico com partition key baseada no `shardKey` |
| Workers | Spring Kafka Consumer | 3 threads consumers, uma por partition; acumulam eventos em buffer |
| Batch flush | `@Scheduled` | A cada N segundos drena os buffers e escreve em batch no Cassandra |
| Repositório | Cassandra Driver | Executa `UNLOGGED BATCH` com `PreparedStatement` reutilizável |
| Banco | Apache Cassandra | Armazena eventos com `shard_id` como partition key |

---

## Conceitos demonstrados

### Sharding com Kafka Partitions

O tópico `write-events` possui **3 partitions**. O producer publica cada mensagem com uma chave explícita calculada como `shardKey % 3`. O Kafka garante que mensagens com a mesma chave sempre vão para a mesma partition — e, portanto, para o mesmo worker.

```
shardKey=0  → partition 0 → Worker-0 → shard_id=0
shardKey=1  → partition 1 → Worker-1 → shard_id=1
shardKey=42 → partition 0 → Worker-0 → shard_id=0  (42 % 3 = 0)
shardKey=7  → partition 1 → Worker-1 → shard_id=1  (7 % 3 = 1)
```

### Agregação em Buffer (Batch Write)

Cada worker mantém uma lista em memória (`CopyOnWriteArrayList`) por shard. Em vez de escrever cada evento individualmente no banco — o que geraria alta pressão de I/O —, o worker acumula eventos e os persiste em lote a cada N segundos.

```
Eventos recebidos → [e1, e2, e3, ..., eN] → UNLOGGED BATCH → Cassandra
```

Isso reduz drasticamente o número de round-trips ao banco e melhora o throughput de escrita.

### Sharding no Cassandra

A tabela `write_events` usa `shard_id` (valores 0, 1 ou 2) como **partition key**. O Cassandra distribui partições entre nós usando consistent hashing (MurmurHash3): cada valor de `shard_id` é hasheado para um token range diferente, direcionando os dados para nós distintos do cluster.

```sql
CREATE TABLE write_events (
    shard_id   INT,       -- partition key → distribui entre nós
    event_id   UUID,      -- clustering key → ordena dentro da partição
    payload    TEXT,
    created_at TIMESTAMP,
    written_at TIMESTAMP,
    PRIMARY KEY (shard_id, event_id)
);
```

### UNLOGGED BATCH

O repositório usa `BatchType.UNLOGGED` para agrupar múltiplos inserts em uma única request ao Cassandra. Como todos os eventos de um batch têm o mesmo `shard_id`, eles vão para o mesmo coordinator e a operação é naturalmente atômica para essa partition — sem o overhead do batchlog replicado do `LOGGED BATCH`.

---

## Stack tecnológica

| Tecnologia | Versão |
|---|---|
| Java | 17 |
| Spring Boot | 4.0.5 |
| Spring Kafka | 4.0.4 |
| Apache Kafka | 4.1 (via kafka-clients) |
| Apache Cassandra Driver | 4.19.2 |
| Cassandra | 4.1 (Docker) |
| Lombok | 1.18.x |
| SpringDoc OpenAPI | 3.0.2 |

---

## Estrutura do projeto

```
src/main/java/com/arturpc/scaling_writes/
├── ScalingWritesApplication.java   # Ponto de entrada + @EnableScheduling
├── config/
│   ├── CassandraConfig.java        # Bootstrap do schema + CqlSession @Primary
│   └── KafkaConfig.java            # Tópico com 3 partitions + KafkaTemplate
├── controller/
│   └── WriteEventController.java   # POST /events · GET /events/stats
├── domain/
│   ├── WriteEvent.java             # DTO trafegado no Kafka
│   └── WriteEventEntity.java       # POJO do registro Cassandra
├── producer/
│   └── WriteEventProducer.java     # Publica com partition key = shardKey % 3
├── consumer/
│   └── WriteEventWorker.java       # 3 consumers + buffers + @Scheduled flush
└── repository/
    └── WriteEventRepository.java   # Batch insert via CqlSession + PreparedStatement
```

---

## Pré-requisitos

- **Docker** e **Docker Compose**
- **Java 17+** (JDK completo com `javac`)
- **Maven** (ou use o wrapper `./mvnw` incluído)

---

## Como executar

### 1. Subir a infraestrutura

```bash
docker compose up -d
```

Aguarde até que o Cassandra esteja saudável (cerca de 60 segundos):

```bash
docker compose ps
# cassandra   Up (healthy)
```

### 2. Iniciar a aplicação

```bash
./mvnw spring-boot:run
```

A aplicação estará disponível em `http://localhost:8080`.

Na inicialização, o `CassandraConfig` cria automaticamente o keyspace `scaling_writes` e a tabela `write_events` caso não existam.

---

## API

### `POST /events` — Publicar evento de escrita

Recebe um evento e o encaminha ao Kafka. O worker responsável irá acumulá-lo em buffer e persistir no Cassandra no próximo flush.

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|---|---|---|
| `shardKey` | `int` | Chave de sharding. Qualquer inteiro; o sistema calcula `shardKey % 3` para determinar a partition/shard. |
| `payload`  | `string` | Conteúdo do evento. |

**Exemplo:**

```bash
curl -X POST "http://localhost:8080/events?shardKey=42&payload=meu-evento"
```

**Resposta (`202 Accepted`):**

```json
{
  "status": "accepted",
  "shardKey": 42,
  "targetPartition": 0,
  "targetShard": 0
}
```

---

### `GET /events/stats` — Estatísticas por shard

Retorna o total de eventos persistidos no Cassandra por shard e quantos ainda estão em buffer aguardando o próximo flush.

**Exemplo:**

```bash
curl http://localhost:8080/events/stats
```

**Resposta (`200 OK`):**

```json
{
  "cassandra_persisted": {
    "shard_0": 1250,
    "shard_1": 1183,
    "shard_2": 1301,
    "total": 3734
  },
  "in_buffer_pending_flush": {
    "shard_0_pending": 47,
    "shard_1_pending": 52,
    "shard_2_pending": 39
  }
}
```

---

### Swagger UI

Documentação interativa disponível em:

```
http://localhost:8080/swagger-ui.html
```

---

## Infraestrutura Docker

O arquivo `docker-compose.yml` sobe os seguintes serviços:

| Serviço | Porta | Descrição |
|---|---|---|
| `zookeeper` | 2181 | Coordenador do Kafka |
| `kafka` | 9092 | Broker Kafka (1 broker, 1 tópico com 3 partitions) |
| `kafdrop` | 9000 | UI web para inspecionar tópicos e mensagens do Kafka |
| `cassandra` | 9042 | Banco de dados (nó único para desenvolvimento) |

### Acessar o Kafdrop

Abra `http://localhost:9000` para visualizar o tópico `write-events`, suas partitions e as mensagens em tempo real.

### Cluster de 3 nós Cassandra (opcional)

Para simular o cluster completo com 3 nós, o Docker precisa de pelo menos **6 GB de memória** configurada.

**Passo 1:** Pare os containers e limpe os volumes (necessário para que o novo nó seed ingresse o cluster com o snitch correto):

```bash
docker compose down -v
```

**Passo 2:** Aumente a memória do Docker em Settings > Resources > Memory para **>= 6 GB**, depois suba com o perfil `cluster`:

```bash
docker compose --profile cluster up -d
```

Aguarde até que todos os 3 nós estejam ativos. Você pode verificar com:

```bash
docker exec cassandra nodetool status
# Resultado esperado: 3 linhas com status "UN" (Up/Normal)
# UN  172.x.x.x  cassandra
# UN  172.x.x.x  cassandra-node2
# UN  172.x.x.x  cassandra-node3
```

**Passo 3:** Altere o fator de replicação no `application.yaml`:

```yaml
app:
  cassandra:
    replication-factor: 3
```

**Passo 4:** Inicie a aplicação normalmente. O `CassandraConfig` criará (ou recriará) o keyspace com `replication_factor=3`, distribuindo réplicas pelos 3 nós.

---

## Configurações

Todas as configurações ficam em `src/main/resources/application.yaml`.

| Propriedade | Padrão | Descrição |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Endereço do broker Kafka |
| `spring.kafka.consumer.group-id` | `scaling-writes-group` | Consumer group dos workers |
| `spring.cassandra.contact-points` | `localhost:9042` | Endereço do nó Cassandra |
| `spring.cassandra.local-datacenter` | `dc1` | Nome do datacenter Cassandra |
| `app.cassandra.keyspace-name` | `scaling_writes` | Keyspace utilizado |
| `app.cassandra.replication-factor` | `1` | Fator de replicação do keyspace |
| `app.batch.flush-interval-ms` | `60000` | Intervalo do flush do buffer (ms) |

---

## Observações sobre o ambiente de desenvolvimento

**Memória do Docker:** A imagem `cassandra:4.1` aloca heap JVM automaticamente com base na RAM disponível, podendo usar mais de 1 GB. O `docker-compose.yml` configura `MAX_HEAP_SIZE=512M` e `mem_limit=1200m` para evitar OOM kills em ambientes com memória limitada.

**Nome do datacenter:** Todos os nós do cluster (seed e adicionais) usam `CASSANDRA_DC: dc1` com `CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch` no `docker-compose.yml`. O `GossipingPropertyFileSnitch` é essencial para que todos os nós se reconheçam como pertencentes ao mesmo DC via gossip protocol. A configuração `local-datacenter: dc1` no `application.yaml` e o nome de DC no `CREATE KEYSPACE` (lido dinamicamente de `${spring.cassandra.local-datacenter}`) devem ser idênticos ao valor de `CASSANDRA_DC` — qualquer inconsistência faz o Cassandra não colocar réplicas em alguns nós, concentrando os dados em um subconjunto do cluster.

**Inicialização do schema:** O driver Cassandra não permite conectar a um keyspace inexistente. O `CassandraConfig` resolve isso abrindo uma sessão bootstrap sem keyspace, criando o schema, e depois fornecendo a sessão definitiva (com keyspace) como bean `@Primary` para toda a aplicação.

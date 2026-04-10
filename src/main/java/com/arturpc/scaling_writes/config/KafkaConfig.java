package com.arturpc.scaling_writes.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuração do Kafka.
 *
 * Tópico "write-events" com 3 partitions:
 *   - Partition 0 → Worker-0 → shard_id=0
 *   - Partition 1 → Worker-1 → shard_id=1
 *   - Partition 2 → Worker-2 → shard_id=2
 *
 * O producer usa (shardKey % 3) como partition key explícita, garantindo
 * que cada consumer thread receba apenas os eventos do seu shard.
 */
@Configuration
public class KafkaConfig {

    public static final String TOPIC_WRITE_EVENTS = "write-events";
    public static final int NUM_PARTITIONS = 3;
    public static final int REPLICATION_FACTOR = 1;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Declara o tópico com 3 partitions.
     * O Spring Kafka Admin cria o tópico automaticamente na inicialização se ele não existir.
     */
    @Bean
    public NewTopic writeEventsTopic() {
        return TopicBuilder.name(TOPIC_WRITE_EVENTS)
                .partitions(NUM_PARTITIONS)
                .replicas(REPLICATION_FACTOR)
                .build();
    }

    /**
     * ProducerFactory com chave do tipo Integer (partition key = shardKey % 3)
     * e valor do tipo WriteEvent serializado como JSON.
     */
    @Bean
    public ProducerFactory<Integer, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        // Garante que apenas UMA mensagem seja enviada por request (sem batching no producer)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * KafkaTemplate tipado para envio com chave Integer.
     */
    @Bean
    public KafkaTemplate<Integer, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}

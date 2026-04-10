package com.arturpc.scaling_writes;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Ponto de entrada da aplicação.
 *
 * @EnableScheduling habilita o processador de @Scheduled, necessário para
 * que o WriteEventWorker execute o flush periódico dos buffers para o Cassandra.
 */
@SpringBootApplication
@EnableScheduling
public class ScalingWritesApplication {

	public static void main(String[] args) {
		SpringApplication.run(ScalingWritesApplication.class, args);
	}

}

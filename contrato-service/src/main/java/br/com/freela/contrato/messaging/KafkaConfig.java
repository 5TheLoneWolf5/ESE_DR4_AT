package br.com.freela.contrato.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    public static final String TOPICO_CONTRATOS_CRIADOS = "contratos.criados";
    public static final String TOPICO_ENTREGAS_REGISTRADAS = "contratos.entregas.registradas";
    public static final String TOPICO_CONTRATOS_CONCLUIDOS = "contratos.concluidos";

    @Bean
    public NewTopic topicContratosCriados() {
        return TopicBuilder.name(TOPICO_CONTRATOS_CRIADOS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topicEntregasRegistradas() {
        return TopicBuilder.name(TOPICO_ENTREGAS_REGISTRADAS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic topicContratosConcluidos() {
        return TopicBuilder.name(TOPICO_CONTRATOS_CONCLUIDOS)
                .partitions(3)
                .replicas(1)
                .build();
    }
}

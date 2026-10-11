package br.com.freela.contrato.messaging;

import br.com.freela.contrato.domain.shared.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Component
public class ContratoPublisher {

    private static final Logger log = LoggerFactory.getLogger(ContratoPublisher.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public ContratoPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<String, String>> publicarEvento(DomainEvent evento) {
        String id = evento.eventId().toString();
        try {
            String json = objectMapper.writeValueAsString(evento);
            return kafkaTemplate.send(evento.eventType(), id, json);
        } catch (Exception e) {
            log.warn("Não foi possivel publicar evento no Kafka: {}, {}", id, e.getMessage());
            throw new RuntimeException("Erro ao serializar evento: ", e);
        }
    }
}
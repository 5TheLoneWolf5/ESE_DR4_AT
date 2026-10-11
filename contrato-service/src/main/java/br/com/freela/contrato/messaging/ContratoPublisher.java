package br.com.freela.contrato.messaging;

import br.com.freela.contrato.domain.shared.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

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
        String eventId = evento.eventId().toString();
        String partitionKey = evento.contratoId().toString();
        try {
            String json = objectMapper.writeValueAsString(evento);
            log.info("contrato.publicar.inicio eventType={} eventId={} contratoId={} correlationId={}",
                    evento.eventType(), eventId, partitionKey, evento.correlationId());
            return kafkaTemplate.send(evento.eventType(), partitionKey, json)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("contrato.publicar.falha eventType={} eventId={} contratoId={} erro={}",
                                    evento.eventType(), eventId, partitionKey, ex.getMessage());
                        } else {
                            log.info("contrato.publicar.sucesso eventType={} eventId={} contratoId={} partition={} offset={}",
                                    evento.eventType(), eventId, partitionKey,
                                    result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                        }
                    });
        } catch (Exception e) {
            log.error("contrato.publicar.erro eventType={} eventId={} contratoId={} erro={}",
                    evento.eventType(), eventId, partitionKey, e.getMessage());
            throw new RuntimeException("Erro ao serializar evento: ", e);
        }
    }
}
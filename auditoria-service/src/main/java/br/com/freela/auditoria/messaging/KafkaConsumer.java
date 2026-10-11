package br.com.freela.auditoria.messaging;

import br.com.freela.auditoria.AuditoriaService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private final AuditoriaService auditoriaService;
    private final ObjectMapper objectMapper;

    public KafkaConsumer(AuditoriaService auditoriaService, @Autowired(required = false) ObjectMapper objectMapper) {
        this.auditoriaService = auditoriaService;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    @KafkaListener(topics = {
            "contratos.criados",
            "contratos.entregas.registradas",
            "contratos.concluidos"
    }, groupId = "auditoria-service-group")
    public void consume(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        String key = record.key();
        String payload = record.value();

        log.info("auditoria.mensagem.recebida topic={} partition={} offset={} key={}",
                topic, record.partition(), record.offset(), key);

        try {
            JsonNode json = objectMapper.readTree(payload);
            UUID eventId = json.has("eventId") ? UUID.fromString(json.get("eventId").asText()) : null;
            UUID aggregateId = json.has("contratoId") ? UUID.fromString(json.get("contratoId").asText()) : null;
            String correlationId = json.has("correlationId") ? json.get("correlationId").asText() : "";
            String eventType = topic;

            log.info("auditoria.evento.processando topic={} eventId={} aggregateId={} correlationId={}",
                    topic, eventId, aggregateId, correlationId);

            boolean gravado = auditoriaService.registrar(eventId, aggregateId, eventType, correlationId, payload);
            log.info("auditoria.processamento.resultado eventId={} aggregateId={} topic={} resultado={}",
                    eventId, aggregateId, topic, gravado ? "SUCESSO" : "IGNORADO_DUPLICADO");
        } catch (Exception e) {
            log.error("auditoria.processamento.falha topic={} key={} erro={}", topic, key, e.getMessage(), e);
            throw new RuntimeException("Falha ao processar mensagem no auditoria-service: " + e.getMessage(), e);
        }
    }
}

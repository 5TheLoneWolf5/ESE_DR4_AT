package br.com.freela.reputacao.messaging;

import br.com.freela.reputacao.ReputacaoService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private final ReputacaoService reputacaoService;
    private final ObjectMapper objectMapper;

    public KafkaConsumer(ReputacaoService reputacaoService, @Autowired(required = false) ObjectMapper objectMapper) {
        this.reputacaoService = reputacaoService;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    @KafkaListener(topics = "contratos.concluidos", groupId = "reputacao-service-group")
    public void consume(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        String key = record.key();
        String payload = record.value();

        log.info("reputacao.mensagem.recebida topic={} partition={} offset={} key={}",
                topic, record.partition(), record.offset(), key);

        try {
            JsonNode json = objectMapper.readTree(payload);
            UUID eventId = json.has("eventId") ? UUID.fromString(json.get("eventId").asText()) : null;
            UUID contratoId = json.has("contratoId") ? UUID.fromString(json.get("contratoId").asText()) : null;
            UUID freelancerId = json.has("freelancerId") ? UUID.fromString(json.get("freelancerId").asText()) : null;
            BigDecimal valor = json.has("valor") ? new BigDecimal(json.get("valor").asText()) : BigDecimal.ZERO;
            String correlationId = json.has("correlationId") ? json.get("correlationId").asText() : "";

            log.info("reputacao.evento.recebido eventType=ContratoConcluido eventId={} contratoId={} freelancerId={} valor={} correlationId={}",
                    eventId, contratoId, freelancerId, valor, correlationId);

            if (freelancerId != null) {
                boolean atualizado = reputacaoService.registrarContratoConcluido(eventId, contratoId, freelancerId, valor, correlationId);
                log.info("reputacao.processamento.resultado eventId={} contratoId={} freelancerId={} resultado={}",
                        eventId, contratoId, freelancerId, atualizado ? "SUCESSO" : "IGNORADO_DUPLICADO");
            } else {
                log.warn("reputacao.evento.sem_freelancer eventId={} contratoId={}", eventId, contratoId);
            }
        } catch (Exception e) {
            log.error("reputacao.processamento.falha topic={} key={} erro={}", topic, key, e.getMessage(), e);
            throw new RuntimeException("Falha ao processar mensagem no reputacao-service: " + e.getMessage(), e);
        }
    }
}

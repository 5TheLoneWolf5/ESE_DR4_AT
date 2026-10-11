package br.com.freela.notificacao.messaging;

import br.com.freela.notificacao.NotificacaoService;
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
    private final NotificacaoService notificacaoService;
    private final ObjectMapper objectMapper;

    public KafkaConsumer(NotificacaoService notificacaoService, @Autowired(required = false) ObjectMapper objectMapper) {
        this.notificacaoService = notificacaoService;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    @KafkaListener(topics = {
            "contratos.criados",
            "contratos.entregas.registradas",
            "contratos.concluidos"
    }, groupId = "notificacao-service-group")
    public void consume(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        String key = record.key();
        String payload = record.value();

        log.info("notificacao.mensagem.recebida topic={} partition={} offset={} key={}",
                topic, record.partition(), record.offset(), key);

        try {
            JsonNode json = objectMapper.readTree(payload);
            UUID eventId = json.has("eventId") ? UUID.fromString(json.get("eventId").asText()) : null;
            UUID contratoId = json.has("contratoId") ? UUID.fromString(json.get("contratoId").asText()) : null;
            UUID clienteId = json.has("clienteId") ? UUID.fromString(json.get("clienteId").asText()) : null;
            UUID freelancerId = json.has("freelancerId") ? UUID.fromString(json.get("freelancerId").asText()) : null;
            String correlationId = json.has("correlationId") ? json.get("correlationId").asText() : "";

            log.info("notificacao.evento.recebido topic={} eventId={} contratoId={} correlationId={}",
                    topic, eventId, contratoId, correlationId);

            boolean processado = false;
            switch (topic) {
                case "contratos.criados" -> {
                    String titulo = json.has("titulo") ? json.get("titulo").asText() : "";
                    String valor = json.has("valor") ? json.get("valor").asText() : "";
                    if (freelancerId != null) {
                        String msg = "Novo contrato proposto: " + titulo + " (Valor: R$ " + valor + ")";
                        processado = notificacaoService.registrar(eventId, contratoId, freelancerId, "CONTRATO_CRIADO", msg);
                        log.info("notificacao.processamento.resultado evento=ContratoCriado contratoId={} destinatarioId={} resultado={}",
                                contratoId, freelancerId, processado ? "SUCESSO" : "DUPLICADO");
                    }
                }
                case "contratos.entregas.registradas" -> {
                    if (clienteId != null) {
                        String msg = "A entrega do contrato foi registrada pelo freelancer. Por favor, revise para aprovação/conclusão.";
                        processado = notificacaoService.registrar(eventId, contratoId, clienteId, "ENTREGA_REGISTRADA", msg);
                        log.info("notificacao.processamento.resultado evento=EntregaRegistrada contratoId={} destinatarioId={} resultado={}",
                                contratoId, clienteId, processado ? "SUCESSO" : "DUPLICADO");
                    }
                }
                case "contratos.concluidos" -> {
                    if (freelancerId != null) {
                        String msg = "O contrato foi concluído e o pagamento foi liberado com sucesso!";
                        processado = notificacaoService.registrar(eventId, contratoId, freelancerId, "CONTRATO_CONCLUIDO", msg);
                        log.info("notificacao.processamento.resultado evento=ContratoConcluido contratoId={} destinatarioId={} resultado={}",
                                contratoId, freelancerId, processado ? "SUCESSO" : "DUPLICADO");
                    }
                }
                default -> log.warn("notificacao.topico.desconhecido topico={}", topic);
            }
        } catch (Exception e) {
            log.error("notificacao.processamento.falha topic={} key={} erro={}", topic, key, e.getMessage(), e);
            throw new RuntimeException("Falha ao processar mensagem no notificacao-service: " + e.getMessage(), e);
        }
    }
}
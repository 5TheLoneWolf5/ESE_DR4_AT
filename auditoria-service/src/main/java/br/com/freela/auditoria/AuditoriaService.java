package br.com.freela.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class AuditoriaService {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);
    private final EventoAuditoriaRepository repository;

    public AuditoriaService(EventoAuditoriaRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public boolean registrar(UUID eventId, UUID aggregateId, String eventType, String correlationId, String payload) {
        if (eventId != null && repository.existsByEventId(eventId)) {
            log.warn("auditoria.evento.duplicado eventId={} aggregateId={} eventType={}", eventId, aggregateId, eventType);
            return false;
        }

        log.info("auditoria.registro.inicio eventId={} aggregateId={} eventType={} correlationId={}",
                eventId, aggregateId, eventType, correlationId);
        EventoAuditoria e = repository.save(new EventoAuditoria(eventId, aggregateId, eventType, correlationId, payload));
        log.info("auditoria.registro.sucesso auditoriaId={} eventId={} aggregateId={} eventType={}",
                e.getId(), eventId, aggregateId, eventType);
        return true;
    }

    @Transactional(readOnly = true)
    public List<EventoAuditoria> listarTodos() {
        return repository.findAll();
    }

    @Transactional(readOnly = true)
    public List<EventoAuditoria> buscarPorContrato(UUID aggregateId) {
        return repository.findByAggregateId(aggregateId);
    }

    @Transactional(readOnly = true)
    public List<EventoAuditoria> buscarPorCorrelationId(String correlationId) {
        return repository.findByCorrelationId(correlationId);
    }
}

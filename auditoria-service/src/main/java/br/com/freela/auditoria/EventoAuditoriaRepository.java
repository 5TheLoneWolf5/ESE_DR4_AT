package br.com.freela.auditoria;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, UUID> {
    boolean existsByEventId(UUID eventId);
    List<EventoAuditoria> findByAggregateId(UUID aggregateId);
    List<EventoAuditoria> findByCorrelationId(String correlationId);
    List<EventoAuditoria> findByEventType(String eventType);
}

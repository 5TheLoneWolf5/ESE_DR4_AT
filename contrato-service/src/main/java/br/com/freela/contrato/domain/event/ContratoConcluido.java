package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ContratoConcluido(UUID eventId, Instant occurredAt, UUID contratoId)
        implements DomainEvent {
    @Override public String eventType() { return "contratos.concluidos"; }
}
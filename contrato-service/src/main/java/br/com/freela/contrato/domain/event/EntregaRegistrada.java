package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.shared.DomainEvent;

import java.time.Instant;
import java.util.UUID;

public record EntregaRegistrada(
        UUID eventId,
        Instant occurredAt,
        UUID contratoId,
        UUID clienteId,
        UUID freelancerId,
        String correlationId
) implements DomainEvent {
    @Override
    public String eventType() {
        return "contratos.entregas.registradas";
    }
}

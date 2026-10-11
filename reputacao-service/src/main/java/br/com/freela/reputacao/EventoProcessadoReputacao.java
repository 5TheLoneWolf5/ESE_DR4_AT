package br.com.freela.reputacao;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "eventos_processados_reputacao")
public class EventoProcessadoReputacao {
    @Id
    private UUID eventId;
    private UUID contratoId;
    private UUID freelancerId;
    private Instant processadoEm;

    protected EventoProcessadoReputacao() {}

    public EventoProcessadoReputacao(UUID eventId, UUID contratoId, UUID freelancerId) {
        this.eventId = eventId;
        this.contratoId = contratoId;
        this.freelancerId = freelancerId;
        this.processadoEm = Instant.now();
    }

    public UUID getEventId() { return eventId; }
    public UUID getContratoId() { return contratoId; }
    public UUID getFreelancerId() { return freelancerId; }
    public Instant getProcessadoEm() { return processadoEm; }
}

package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.shared.DomainEvent;
import utils.CorrelationId;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class Contrato {
    private final UUID id;
    private final UUID clienteId;
    private final UUID freelancerId;
    private final String titulo;
    private final BigDecimal valor;
    private StatusContrato status;
    private final Instant criadoEm;
    private final List<DomainEvent> domainEvents = new ArrayList<>();

    private Contrato(UUID id, UUID clienteId, UUID freelancerId, String titulo,
                     BigDecimal valor, StatusContrato status, Instant criadoEm) {
        this.id = id;
        this.clienteId = clienteId;
        this.freelancerId = freelancerId;
        this.titulo = titulo;
        this.valor = valor;
        this.status = status;
        this.criadoEm = criadoEm;
    }

    public static Contrato criar(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor) {
        if (clienteId == null || freelancerId == null) throw new IllegalArgumentException("Cliente e freelancer são obrigatórios");
        if (titulo == null || titulo.isBlank()) throw new IllegalArgumentException("Título é obrigatório");
        if (valor == null || valor.signum() <= 0) throw new IllegalArgumentException("Valor deve ser positivo");

        UUID idContrato = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String correlationId = CorrelationId.getCorrelationId();
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }

        var contrato = new Contrato(idContrato, clienteId, freelancerId, titulo.trim(), valor,
                StatusContrato.ATIVO, Instant.now());

        contrato.domainEvents.add(
                new ContratoCriado(eventId, Instant.now(), idContrato, clienteId, freelancerId, titulo.trim(), valor, correlationId)
        );

        return contrato;
    }

    public static Contrato restaurar(UUID id, UUID clienteId, UUID freelancerId, String titulo,
                                     BigDecimal valor, StatusContrato status, Instant criadoEm) {
        return new Contrato(id, clienteId, freelancerId, titulo, valor, status, criadoEm);
    }

    public void registrarEntrega() {
        if (status != StatusContrato.ATIVO) throw new IllegalStateException("Somente contratos ativos recebem entrega");

        String correlationId = CorrelationId.getCorrelationId();
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        status = StatusContrato.ENTREGA_REGISTRADA;

        domainEvents.add(new EntregaRegistrada(UUID.randomUUID(), Instant.now(), this.id, this.clienteId, this.freelancerId, correlationId));
    }

    public void concluir() {
        if (status != StatusContrato.ENTREGA_REGISTRADA) throw new IllegalStateException("A entrega precisa estar registrada");

        String correlationId = CorrelationId.getCorrelationId();
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = UUID.randomUUID().toString();
        }
        status = StatusContrato.CONCLUIDO;

        domainEvents.add(new ContratoConcluido(UUID.randomUUID(), Instant.now(), this.id, this.clienteId, this.freelancerId, this.valor, correlationId));
    }

    public void cancelar() {
        if (status == StatusContrato.CONCLUIDO) throw new IllegalStateException("Contrato concluído não pode ser cancelado");
        status = StatusContrato.CANCELADO;
    }

    public List<DomainEvent> pullDomainEvents() {
        var copy = List.copyOf(domainEvents);
        domainEvents.clear();
        return copy;
    }

    public List<DomainEvent> domainEvents() {
        return Collections.unmodifiableList(domainEvents);
    }

    public UUID id(){return id;}
    public UUID clienteId(){return clienteId;}
    public UUID freelancerId(){return freelancerId;}
    public String titulo(){return titulo;}
    public BigDecimal valor(){return valor;}
    public StatusContrato status(){return status;}
    public Instant criadoEm(){return criadoEm;}
}
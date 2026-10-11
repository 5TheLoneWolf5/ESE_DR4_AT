package br.com.freela.reputacao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class ReputacaoService {
    private static final Logger log = LoggerFactory.getLogger(ReputacaoService.class);
    private final ReputacaoRepository repository;
    private final EventoProcessadoReputacaoRepository eventoProcessadoRepository;

    public ReputacaoService(ReputacaoRepository repository, EventoProcessadoReputacaoRepository eventoProcessadoRepository) {
        this.repository = repository;
        this.eventoProcessadoRepository = eventoProcessadoRepository;
    }

    @Transactional
    public boolean registrarContratoConcluido(UUID eventId, UUID contratoId, UUID freelancerId, BigDecimal valor, String correlationId) {
        if (eventId != null && eventoProcessadoRepository.existsById(eventId)) {
            log.warn("reputacao.evento.duplicado eventId={} contratoId={} freelancerId={}", eventId, contratoId, freelancerId);
            return false;
        }

        log.info("reputacao.atualizacao.inicio eventId={} contratoId={} freelancerId={} valor={} correlationId={}",
                eventId, contratoId, freelancerId, valor, correlationId);

        var r = repository.findById(freelancerId).orElseGet(() -> new ReputacaoFreelancer(freelancerId));
        r.registrarContrato(valor);
        repository.save(r);

        if (eventId != null) {
            eventoProcessadoRepository.save(new EventoProcessadoReputacao(eventId, contratoId, freelancerId));
        }

        log.info("reputacao.atualizacao.sucesso eventId={} contratoId={} freelancerId={} contratosConcluidos={} valorTotal={}",
                eventId, contratoId, freelancerId, r.getContratosConcluidos(), r.getValorTotal());
        return true;
    }

    @Transactional(readOnly = true)
    public ReputacaoFreelancer buscarPorFreelancerId(UUID freelancerId) {
        return repository.findById(freelancerId).orElse(null);
    }
}

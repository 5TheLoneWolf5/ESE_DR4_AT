package br.com.freela.reputacao;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface EventoProcessadoReputacaoRepository extends JpaRepository<EventoProcessadoReputacao, UUID> {
}

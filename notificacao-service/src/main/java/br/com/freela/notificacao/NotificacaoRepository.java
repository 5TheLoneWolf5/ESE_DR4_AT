package br.com.freela.notificacao;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface NotificacaoRepository extends JpaRepository<Notificacao, UUID> {
    boolean existsByEventId(UUID eventId);
    List<Notificacao> findByContratoId(UUID contratoId);
    List<Notificacao> findByDestinatarioId(UUID destinatarioId);
}

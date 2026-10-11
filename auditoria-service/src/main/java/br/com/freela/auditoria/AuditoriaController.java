package br.com.freela.auditoria;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/auditoria")
public class AuditoriaController {
    private static final Logger log = LoggerFactory.getLogger(AuditoriaController.class);
    private final AuditoriaService auditoriaService;

    public AuditoriaController(AuditoriaService auditoriaService) {
        this.auditoriaService = auditoriaService;
    }

    @GetMapping
    public List<EventoAuditoria> listar(
            @RequestParam(required = false) UUID contratoId,
            @RequestParam(required = false) String correlationId,
            @RequestHeader(value = "X-Correlation-Id", required = false) String headerCorrelationId
    ) {
        String corrId = (correlationId != null && !correlationId.isBlank()) ? correlationId : headerCorrelationId;
        log.info("http.auditoria.listar contratoId={} correlationId={}", contratoId, corrId);

        if (contratoId != null) {
            return auditoriaService.buscarPorContrato(contratoId);
        }
        if (corrId != null && !corrId.isBlank()) {
            return auditoriaService.buscarPorCorrelationId(corrId);
        }
        return auditoriaService.listarTodos();
    }
}

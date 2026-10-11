package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import jakarta.servlet.http.HttpServletRequest;
import java.util.UUID;

public class CorrelationId {

    private static final String CORRELATION_ID_MDC = "correlationId";
    private static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    private static final Logger logger = LoggerFactory.getLogger(CorrelationId.class);

    public static String getCorrelationId() {
        String correlationId = MDC.get(CORRELATION_ID_MDC);
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = getCorrelationIdFromRequest();
        }
        return correlationId;
    }

    public static UUID getCorrelationIdAsUUID() {
        String correlationId = getCorrelationId();
        if (correlationId != null && !correlationId.isEmpty()) {
            try {
                return UUID.fromString(correlationId);
            } catch (IllegalArgumentException e) {
                return UUID.randomUUID();
            }
        }
        return UUID.randomUUID();
    }

    public static String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    private static String getCorrelationIdFromRequest() {
        try {
            ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attributes != null) {
                HttpServletRequest request = attributes.getRequest();
                return request.getHeader(CORRELATION_ID_HEADER);
            }
        } catch (Exception e) {
            logger.warn("Não foi possível obter o Correlation ID do contexto da requisição: {}", e.getMessage(), e);
        }
        return null;
    }

    public static void clear() {
        MDC.remove(CORRELATION_ID_MDC);
    }

    public static void setCorrelationId(String correlationId) {
        if (correlationId != null && !correlationId.isEmpty()) {
            MDC.put(CORRELATION_ID_MDC, correlationId);
        }
    }
}

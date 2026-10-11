# Documentação da Solução — Marketplace de Freelancers (Assessment)

Este documento descreve a arquitetura, especificação de mensagens, garantias de concorrência e ordenação, estratégia de idempotência, observabilidade e instruções de execução da plataforma de microsserviços orientada a eventos.

---

## 1. Visão Geral da Arquitetura

A plataforma **Freela Marketplace** adota uma arquitetura de microsserviços desacoplada com comunicação assíncrona orientada a eventos via **Apache Kafka (modo KRaft)**, orquestração de rotas via **Spring Cloud Gateway**, service discovery via **Netflix Eureka**, bancos de dados isolados no **PostgreSQL** e rastreamento distribuído via **Zipkin**.

```text
                        +--------------------+
                        |  Cliente / Swagger |
                        +---------+----------+
                                  |
                                  | HTTP (X-Correlation-Id)
                                  v
                        +--------------------+
                        |    API Gateway     |
                        |      (:8080)       |
                        +---------+----------+
                                  |
          +-----------------------+-----------------------+
          |                       |                       |
          v                       v                       v
+--------------------+  +--------------------+  +--------------------+
|  contrato-service  |  | notificacao-service|  | reputacao-service  |
|      (:8081)       |  |      (:8082)       |  |      (:8083)       |
+---------+----------+  +---------+----------+  +---------+----------+
          |                       ^                       ^
          | Kafka Producer        | Kafka Consumer        | Kafka Consumer
          v                       |                       |
+--------------------------------------------------------------------+
|                       Apache Kafka (:9092)                         |
|   Tópicos: contratos.criados | contratos.entregas.registradas      |
|            contratos.concluidos                                    |
+--------------------------------------------------------------------+
          |
          | Kafka Consumer
          v
+--------------------+
| auditoria-service  |
|      (:8084)       |
+--------------------+
```

---

## 2. Integração dos Serviços

### 2.1. Notificação (`notificacao-service`)
- **Responsabilidade**: Reagir ao ciclo de vida dos contratos e gerar alertas para clientes e freelancers.
- **Tópicos Consumidos**:
  - `contratos.criados`: Notifica o freelancer sobre a nova oportunidade de trabalho.
  - `contratos.entregas.registradas`: Notifica o cliente que o trabalho foi entregue e aguarda validação.
  - `contratos.concluidos`: Notifica o freelancer sobre a aprovação e liberação de pagamento.
- **Estrutura de Log Contextual**:
  ```text
  notificacao.mensagem.recebida topic=... partition=... offset=... key=...
  notificacao.evento.recebido topic=... eventId=... contratoId=... correlationId=...
  notificacao.processamento.resultado evento=... contratoId=... destinatarioId=... resultado=SUCESSO|DUPLICADO
  ```
- **Persistência**: Banco `notificacao_db`, tabela `notificacoes`.

### 2.2. Reputação (`reputacao-service`)
- **Responsabilidade**: Reagir aos eventos com impacto na pontuação, volume financeiro e histórico do profissional.
- **Tópico Consumido**: `contratos.concluidos`.
- **Regra de Negócio**: Incrementa `contratosConcluidos` e soma o `valor` ao `valorTotal` faturado pelo freelancer.
- **Estrutura de Log Contextual**:
  ```text
  reputacao.evento.recebido eventType=ContratoConcluido eventId=... contratoId=... freelancerId=... valor=...
  reputacao.atualizacao.sucesso eventId=... contratoId=... freelancerId=... contratosConcluidos=... valorTotal=...
  ```
- **Persistência**: Banco `reputacao_db`, tabelas `reputacoes` e `eventos_processados_reputacao`.

### 2.3. Auditoria (`auditoria-service`)
- **Responsabilidade**: Manter a trilha completa e imutável de todos os eventos de negócio transitados pela plataforma.
- **Tópicos Consumidos**: `contratos.criados`, `contratos.entregas.registradas`, `contratos.concluidos`.
- **Campos Auditados**:
  - `id`: Identificador único do registro de auditoria.
  - `eventId`: Identificador global do evento original.
  - `aggregateId`: Identificador do contrato associado.
  - `eventType`: Nome do tópico / tipo do evento.
  - `correlationId`: Identificador de correlação para rastreio fim a fim.
  - `payload`: Conteúdo JSON integral da mensagem.
  - `recebidoEm`: Timestamp de recepção pelo serviço.
- **Persistência**: Banco `auditoria_db`, tabela `auditoria_eventos`.
- **Consultas**: Disponíveis via `GET /api/auditoria`, com filtros opcionais por `contratoId` e `correlationId`.

---

## 3. Especificação das Mensagens (Contratos de Eventos)

### 3.1. Evento: `ContratoCriado`
- **Tópico Kafka**: `contratos.criados`
- **Produtor**: `contrato-service`
- **Consumidores**: `notificacao-service`, `auditoria-service`
- **Chave de Particionamento (Key)**: `contratoId` (String UUID)
- **Campos Obrigatórios**: `eventId`, `occurredAt`, `contratoId`, `clienteId`, `freelancerId`, `titulo`, `valor`, `correlationId`
- **Exemplo de Payload**:
```json
{
  "eventId": "a1b2c3d4-e5f6-7890-abcd-111122223333",
  "occurredAt": "2026-10-11T20:30:00.000Z",
  "contratoId": "7aa6fffa-00f3-4e1c-bafb-6acfcb5d78e4",
  "clienteId": "51e32588-1b1d-4620-865c-fa3f69a47579",
  "freelancerId": "78f04639-7d32-40c4-8488-c0b1757b4421",
  "titulo": "Desenvolvimento de App Mobile",
  "valor": 7500.00,
  "correlationId": "req-987654321"
}
```

### 3.2. Evento: `EntregaRegistrada`
- **Tópico Kafka**: `contratos.entregas.registradas`
- **Produtor**: `contrato-service`
- **Consumidores**: `notificacao-service`, `auditoria-service`
- **Chave de Particionamento (Key)**: `contratoId` (String UUID)
- **Campos Obrigatórios**: `eventId`, `occurredAt`, `contratoId`, `clienteId`, `freelancerId`, `correlationId`
- **Exemplo de Payload**:
```json
{
  "eventId": "b2c3d4e5-f6a7-8901-bcde-222233334444",
  "occurredAt": "2026-10-11T20:45:00.000Z",
  "contratoId": "7aa6fffa-00f3-4e1c-bafb-6acfcb5d78e4",
  "clienteId": "51e32588-1b1d-4620-865c-fa3f69a47579",
  "freelancerId": "78f04639-7d32-40c4-8488-c0b1757b4421",
  "correlationId": "req-987654322"
}
```

### 3.3. Evento: `ContratoConcluido`
- **Tópico Kafka**: `contratos.concluidos`
- **Produtor**: `contrato-service`
- **Consumidores**: `reputacao-service`, `notificacao-service`, `auditoria-service`
- **Chave de Particionamento (Key)**: `contratoId` (String UUID)
- **Campos Obrigatórios**: `eventId`, `occurredAt`, `contratoId`, `clienteId`, `freelancerId`, `valor`, `correlationId`
- **Exemplo de Payload**:
```json
{
  "eventId": "c3d4e5f6-a7b8-9012-cdef-333344445555",
  "occurredAt": "2026-10-11T21:00:00.000Z",
  "contratoId": "7aa6fffa-00f3-4e1c-bafb-6acfcb5d78e4",
  "clienteId": "51e32588-1b1d-4620-865c-fa3f69a47579",
  "freelancerId": "78f04639-7d32-40c4-8488-c0b1757b4421",
  "valor": 7500.00,
  "correlationId": "req-987654323"
}
```

---

## 4. Processamento Concorrente e Ordenação

### Estratégia de Particionamento
1. **Chave Baseada no Agregado**: No `ContratoPublisher`, a publicação utiliza `contratoId.toString()` como a **message key** do Kafka:
   ```java
   kafkaTemplate.send(evento.eventType(), evento.contratoId().toString(), json);
   ```
2. **Garantia de Ordem Estrita**: Como o Kafka utiliza o algoritmo determinístico Murmur2 para mapear chaves para partições, **todos os eventos pertencentes ao mesmo contrato são obrigatoriamente roteados para a mesma partição**.
3. **Sequência Preservada**: Dentro de uma mesma partição, o Kafka preserva ordem sequencial estrita:
   $$\text{ContratoCriado} \longrightarrow \text{EntregaRegistrada} \longrightarrow \text{ContratoConcluido}$$
   Nenhum consumidor consegue ler a entrega ou a conclusão antes da criação do contrato correspondente.

### Concorrência sem Violação de Ordem
1. **Tópicos Multi-Partição**: Os tópicos são provisionados com **3 partições**.
2. **Consumo Paralelo (`concurrency = 3`)**: Cada microsserviço consumidor configura sua fábrica de ouvintes com concorrência correspondente:
   ```java
   factory.setConcurrency(3);
   ```
   Dessa forma, mensagens de contratos distintos alocadas em partições diferentes são processadas em paralelo por threads independentes, enquanto mensagens do mesmo contrato permanecem enfileiradas e ordenadas na mesma partição.

---

## 5. Tratamento de Mensagens Duplicadas (Idempotência)

Em sistemas distribuídos, a entrega de mensagens pelo broker é do tipo *at-least-once*, o que pode provocar reentregas em cenários de rebalanceamento ou retentativas de rede.

### Mecanismos Implementados:
1. **Identificador Global Único (`eventId`)**:
   Cada evento produzido pelo `contrato-service` carrega um `eventId` gerado uma única vez no momento da alteração de estado.
2. **Idempotência no `notificacao-service`**:
   - O repositório verifica `repository.existsByEventId(eventId)`.
   - Se já existir, a notificação não é replicada e um log com `resultado=DUPLICADO` é emitido.
3. **Idempotência no `reputacao-service`**:
   - Utiliza a tabela de controle `eventos_processados_reputacao` (`EventoProcessadoReputacao`).
   - Se o `eventId` já foi persistido, a operação é ignorada e os contadores de `contratosConcluidos` e `valorTotal` **não sofrem incremento indevido**.
4. **Idempotência no `auditoria-service`**:
   - O repositório verifica `existsByEventId(eventId)`. Mensagens duplicadas não geram registros redundantes na trilha histórica.

---

## 6. Logs da Aplicação e Padronização Contextual

Todos os microsserviços utilizam o formato padronizado de logs via `application.yml`:
```yaml
logging:
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level service=${spring.application.name} thread=%thread logger=%logger{36} - %msg%n"
```

### Campos Contextuais Registrados:
- `service`: Nome do microsserviço executor.
- `contratoId`: Identificador do contrato relacionado.
- `eventId`: Identificador do evento processado.
- `correlationId`: Identificador de correlação transversal.
- `destinatarioId`: Alvo da notificação.
- `resultado`: Desfecho da ação (`SUCESSO`, `DUPLICADO`, `FALHA`).

---

## 7. Centralização de Logs e Rastreamento Distribuído

1. **Correlação End-to-End**:
   - O `api-gateway` intercepta ou gera o cabeçalho `X-Correlation-Id`.
   - O `contrato-service` armazena o valor no MDC e o propaga para o payload dos eventos Kafka.
   - Os serviços consumidores (`notificacao-service`, `reputacao-service`, `auditoria-service`) extraem o `correlationId` da mensagem e registram em seus logs locais e auditoria.
   - Qualquer operação pode ser correlacionada em toda a plataforma buscando por `correlationId`, `contratoId` ou `eventId`.

2. **Distributed Tracing (Zipkin)**:
   - Configurado via Micrometer Tracing Bridge Brave com exportação para o Zipkin local em `http://localhost:9411`.

---

## 8. Guia de Execução e Testes

### 8.1. Inicializar Infraestrutura
```powershell
cd infra
docker compose up -d
```

### 8.2. Compilar os Serviços
Na raiz do repositório:
```powershell
mvn clean compile
```

### 8.3. Executar as Aplicações
Em terminais separados (ou via IDE):
```powershell
# 1. Service Discovery
mvn -pl eureka-server spring-boot:run

# 2. Gateway
mvn -pl api-gateway spring-boot:run

# 3. Produtor de Contratos
mvn -pl contrato-service spring-boot:run

# 4. Consumidores Auxiliares
mvn -pl notificacao-service spring-boot:run
mvn -pl reputacao-service spring-boot:run
mvn -pl auditoria-service spring-boot:run
```

### 8.4. Chamadas de Teste do Ciclo de Vida do Contrato

#### 1. Criar Contrato:
```bash
curl -i -X POST http://localhost:8080/api/contratos \
  -H "Content-Type: application/json" \
  -H "X-Correlation-Id: fluxo-teste-001" \
  -d '{
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "Implementação Kafka Assessment",
    "valor": 4500.00
  }'
```

#### 2. Registrar Entrega:
```bash
curl -i -X POST http://localhost:8080/api/contratos/{CONTRATO_ID}/entrega \
  -H "X-Correlation-Id: fluxo-teste-001"
```

#### 3. Concluir Contrato:
```bash
curl -i -X POST http://localhost:8080/api/contratos/{CONTRATO_ID}/conclusao \
  -H "X-Correlation-Id: fluxo-teste-001"
```

#### 4. Consultar Resultados nos Serviços Consumidores:
- **Notificações**:
  ```bash
  curl http://localhost:8080/api/notificacoes
  ```
- **Reputação**:
  ```bash
  curl http://localhost:8080/api/reputacoes
  ```
- **Auditoria**:
  ```bash
  curl http://localhost:8080/api/auditoria?contratoId={CONTRATO_ID}
  ```
- **Painel Kafka UI**: `http://localhost:8090`
- **Zipkin Tracing**: `http://localhost:9411`

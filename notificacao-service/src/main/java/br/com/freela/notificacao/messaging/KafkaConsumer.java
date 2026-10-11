package br.com.freela.notificacao.messaging;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "contratos.criados", groupId = "br.com.freela")
    public void consume(String message)
    {
        System.out.println("message = " + message);
    }
}
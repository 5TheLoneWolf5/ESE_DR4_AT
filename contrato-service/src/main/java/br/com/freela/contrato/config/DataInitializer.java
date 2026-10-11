package br.com.freela.contrato.config;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class DataInitializer implements CommandLineRunner {
    private final ContratoApplicationService contratoService;

    public DataInitializer(ContratoApplicationService contratoService) {
        this.contratoService = contratoService;
    }

    @Override
    public void run(String... args) {
        contratoService.criar(new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "Espada Rara", new BigDecimal("5000")));
        contratoService.criar(new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "Mansão de Luxo", new BigDecimal("10000000")));
        contratoService.criar(new CriarContratoCommand(UUID.randomUUID(), UUID.randomUUID(), "Carro Sport", new BigDecimal("100000")));
    }
}

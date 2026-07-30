package com.workflowengine.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot 4's own Jackson autoconfiguration provides Jackson 3
 * (tools.jackson.databind.ObjectMapper) — a different class from the Jackson 2
 * (com.fasterxml.jackson.databind.ObjectMapper) this codebase actually uses for outbox payload
 * serialization. This bean fills that gap explicitly rather than relying on autoconfiguration to
 * supply the wrong major version. JavaTimeModule is required since plain ObjectMapper() can't
 * serialize java.time.Instant (used throughout the event records) without it.
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}

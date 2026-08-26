package com.example.nabatvoting.infrastructure.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Enables {@code @Scheduled}, which the outbox relay needs to drain its backlog.
 *
 * <p>Its own class rather than an annotation on the application class: scheduling is
 * infrastructure, and a test that wants the relay quiet can exclude this without also
 * excluding the entrypoint.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}

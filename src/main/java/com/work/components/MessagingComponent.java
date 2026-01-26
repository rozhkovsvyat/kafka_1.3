package com.work.components;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessagingComponent {
    protected static final Logger logger = LoggerFactory.getLogger(MessagingComponent.class);

    protected static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "127.0.0.1:9094");
    protected static final String SCHEMA_REGISTRY = System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://127.0.0.1:8081");
    protected static final String TOPIC = System.getenv().getOrDefault("TOPIC", "test-topic");
    
    // Периодичность отправки сообщений продюсером в мс.
    protected static final int SENDING_PERIOD_MS = Integer.parseInt(System.getenv().getOrDefault("SENDING_PERIOD_MS", "500"));
}

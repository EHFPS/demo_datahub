package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ClientManagerTest {

    @Test
    @Disabled //TODO fix it
    void clientLifecycle() {
        var client = mock(KafkaClient.class);
        var factory = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_CSV.name()))
                .override(KafkaClient.class, client)
                .build();
        var clientManager = factory.getInstance(ClientManager.class);
        clientManager.run();
        verify(client).init();
        verify(client).run();
        verify(client).stop();
    }
}
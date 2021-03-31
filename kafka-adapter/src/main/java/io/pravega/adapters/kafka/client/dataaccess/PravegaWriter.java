/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.adapters.kafka.client.dataaccess;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PravegaWriter<T> implements Writer<T> {

    @NonNull
    private final String scope;

    @NonNull
    private final String stream;

    @NonNull
    private final String controllerUri;

    @NonNull
    private final Serializer<T> serializer;

    private final Integer numSegments;

    private StreamManager streamManager;
    private EventStreamClientFactory clientFactory;
    private EventStreamWriter<T> writer;
    private boolean isClosed = false;

    private boolean isInitialized() {
        return writer != null;
    }

    @Override
    public void init() {
        if (isInitialized()) {
            log.debug("Already initialized");
            return;
        }
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(URI.create(controllerUri))
                .build();

        streamManager = StreamManager.create(clientConfig);

        boolean isScopeCreated = true; //streamManager.createScope(scope);
        if (isScopeCreated) {
            log.info("Created scope {}", scope);
        } else {
            log.debug("Scope {} was already created previously", scope);
        }

        boolean isStreamCreated = streamManager.createStream(scope, stream, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments.intValue() > 0 ? numSegments.intValue() : 1))
                .build());

        if (isStreamCreated) {
            log.info("Created stream {} in scope {}", stream, scope);
        } else {
            log.debug("Stream {} in scope {} was already created previously {}", stream, scope);
        }

        clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        writer = clientFactory.createEventWriter(stream, serializer, EventWriterConfig.builder().build());
        log.debug("Creating a writer for scope/stream: {}/{}", scope, stream);
    }

    @Override
    public CompletableFuture<Void> writeEvent(T event) {
        if (isClosed) {
            throw new IllegalStateException("Already closed");
        }

        if (!isInitialized()) {
            log.info("Not initialized already, initializing");
            this.init();
        }
        return writer.writeEvent(event);
    }

    @Override
    public void flush() {
        if (isClosed) {
            return;
        }
        log.debug("Flushing...");
        writer.flush();
    }

    @Override
    public void close() {
        isClosed = true;
        if (!isInitialized()) {
            return;
        }
        log.debug("Closing...");
        writer.close();
        clientFactory.close();
        streamManager.close();
    }
}

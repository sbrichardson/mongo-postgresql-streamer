package com.malt.mongopostgresqlstreamer;

import lombok.extern.slf4j.Slf4j;
import org.bson.BsonTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Optional;

@SpringBootApplication
@Slf4j
public class StreamerApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(StreamerApplication.class, args);
    }

    @Value("${mongo.connector.forcereimport:false}")
    private boolean forceReimport;

    @Autowired
    private OplogStreamer oplogStreamer;
    @Autowired
    private InitialImporter initialImporter;
    @Autowired
    private CheckpointManager checkpointManager;

    @Override
    public void run(ApplicationArguments args) {
        Optional<BsonTimestamp> checkpoint = checkpointManager.getLastKnown();

        if (!checkpoint.isPresent() || forceReimport) {
            log.info("No checkpoint found, we will perform a initial load");
            checkpoint = Optional.of(checkpointManager.getLastOplog());
            log.info("Last oplog found have timestamp : {}", checkpoint.get().toString());
            initialImporter.start();
            checkpointManager.keep(checkpoint.get());
        }

        oplogStreamer.watchFromCheckpoint(checkpoint);
    }

}

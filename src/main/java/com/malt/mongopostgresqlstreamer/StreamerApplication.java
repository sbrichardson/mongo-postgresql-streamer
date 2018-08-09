package com.malt.mongopostgresqlstreamer;

import org.bson.BsonTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.util.Optional;

@SpringBootApplication
public class StreamerApplication implements ApplicationRunner {

    public static void main(String[] args) {
        SpringApplication.run(StreamerApplication.class, args);
    }

    @Autowired
    private OplogStreamer oplogStreamer;
    @Autowired
    private InitialImporter initialImporter;
    @Autowired
    private CheckpointManager checkpointManager;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Optional<BsonTimestamp> checkpoint = checkpointManager.getLastKnown();

        if (!checkpoint.isPresent()) {
            initialImporter.start();
        }

        oplogStreamer.watchFromCheckpoint(checkpoint);
    }

}

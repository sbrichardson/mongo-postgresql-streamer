package com.malt.mongopostgresqlstreamer;

import org.bson.BsonTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.util.Optional;

@SpringBootApplication
public class StreamerApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamerApplication.class, args);
    }

    @Autowired
    private MappingsManager mappingsManager;
    @Autowired
    private OplogStreamer oplogStreamer;
    @Autowired
    private CheckpointManager checkpointManager;

    @PostConstruct
    public void start() {
        Optional<BsonTimestamp> checkpoint = checkpointManager.getLastKnown();
        oplogStreamer.watchFromCheckpoint(checkpoint);
    }

}

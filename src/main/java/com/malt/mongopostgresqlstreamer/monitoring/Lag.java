package com.malt.mongopostgresqlstreamer.monitoring;

import lombok.Data;
import org.bson.BsonTimestamp;

import java.util.Date;
import java.util.Optional;

@Data
public class Lag {
    private Date lastCheckpoint;
    private Date now;
    private long lagLength;

    public void computeFromCheckpointAndOplog(Optional<BsonTimestamp> lastKnown) {
        lastKnown.ifPresent( checkpoint -> {
            lastCheckpoint = new Date(((long)checkpoint.getTime())*1000);
            now = new Date();
            lagLength = now.getTime() - checkpoint.getTime();
        });
    }
}

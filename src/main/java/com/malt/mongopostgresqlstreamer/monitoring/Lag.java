package com.malt.mongopostgresqlstreamer.monitoring;

import lombok.Data;
import org.bson.BsonTimestamp;

import java.util.Date;
import java.util.Optional;

@Data
public class Lag {
    private Date lastCheckpoint;
    private Date currentOplog;
    private long lagCount;
    private long lagLength;

    public void computeFromCheckpointAndOplog(Optional<BsonTimestamp> lastKnown, BsonTimestamp lastOplog, long count) {
        lastKnown.ifPresent( checkpoint -> {
            lastCheckpoint = new Date(((long)checkpoint.getTime())*1000);
            currentOplog = new Date(((long)lastOplog.getTime())*1000);
            if (lastOplog.getTime() > checkpoint.getTime()) {
                lagLength = lastOplog.getTime() - checkpoint.getTime();
            }
            lagCount = count;
        });
    }
}

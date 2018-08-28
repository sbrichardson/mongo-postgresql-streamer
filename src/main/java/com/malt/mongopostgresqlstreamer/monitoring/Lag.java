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

    public void computeFromCheckpointAndOplog(Optional<BsonTimestamp> lastKnown, Optional<BsonTimestamp> lastOplog, long count) {
        lastKnown.ifPresent( checkpoint -> {
            lastCheckpoint = new Date(((long)checkpoint.getTime())*1000);

            currentOplog = lastOplog.map( t -> new Date(((long)t.getTime())*1000)).orElse(null);
            lastOplog.ifPresent(last -> {
                if (last.getTime() > checkpoint.getTime()) {
                    lagLength = last.getTime() - checkpoint.getTime();
                }
            });
            lagCount = count;
        });
    }
}

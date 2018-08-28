package com.malt.mongopostgresqlstreamer.monitoring;

import com.malt.mongopostgresqlstreamer.CheckpointManager;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.BsonTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class StatusHealthIndicator implements HealthIndicator {

    @Autowired
    private CheckpointManager checkpointManager;

    @Override
    public Health health() {

        Lag lag = new Lag();
        Optional<BsonTimestamp> lastKnown = checkpointManager.getLastKnown();
        BsonTimestamp lastOplog = checkpointManager.getLastOplogForMappedCollections();
        long count = checkpointManager.countSinceTsForMappedCollections(lastKnown);
        lag.computeFromCheckpointAndOplog(lastKnown, lastOplog, count);
        InitialImport initialImport = checkpointManager.lastImportStatus();
        return Health.up()
                .withDetail("lag", lag)
                .withDetail("initial", initialImport)
                .build();
    }
}
package com.malt.mongopostgresqlstreamer.monitoring;

import com.malt.mongopostgresqlstreamer.CheckpointManager;
import org.bson.BsonTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
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
        lag.computeFromCheckpointAndOplog(lastKnown);
        InitialImport initialImport = checkpointManager.lastImportStatus();
        return Health.up()
                .withDetail("lag", lag)
                .withDetail("initial", initialImport)
                .build();
    }
}
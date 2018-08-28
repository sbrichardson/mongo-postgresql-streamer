package com.malt.mongopostgresqlstreamer;

import com.malt.mongopostgresqlstreamer.monitoring.InitialImport;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;

@Service
@Slf4j
public class CheckpointManager {

    public static final String OPLOG_COLLECTION_NAME = "oplog.rs";

    @Value(value = "${mongo.connector.identifier:test}")
    private String identifier;

    @Autowired
    private MappingsManager mappingsManager;
    @Autowired
    @Qualifier("database")
    private MongoDatabase database;
    @Autowired
    @Qualifier("oplog")
    private MongoDatabase oplog;

    public long countSinceTsForMappedCollections(Optional<BsonTimestamp> oCheckpoint) {
        return oCheckpoint.map(checkpoint -> {
            List<String> namespaces = mappingsManager.mappedNamespaces();
            return oplog.getCollection(OPLOG_COLLECTION_NAME).count(
                    and(
                            in("ns", namespaces),
                            gt("ts", checkpoint.getValue())
                    )
            );
        }).orElse(0L);

    }

    public Optional<BsonTimestamp> getLastOplogForMappedCollections() {
        List<String> namespaces = mappingsManager.mappedNamespaces();
        Document lastOplog = oplog.getCollection(OPLOG_COLLECTION_NAME).find(in("ns", namespaces))
                .sort(Sorts.descending("$natural"))
                .first();

        if (lastOplog != null && lastOplog.get("ts", BsonTimestamp.class) != null) {
            return Optional.ofNullable(lastOplog.get("ts", BsonTimestamp.class));
        }
        return Optional.empty();
    }


    public BsonTimestamp getLastOplog() {
        Document lastOplog = oplog.getCollection(OPLOG_COLLECTION_NAME).find()
                .sort(Sorts.descending("$natural"))
                .first();

        if (lastOplog != null) {
            return lastOplog.get("ts", BsonTimestamp.class);
        }
        throw new IllegalStateException("Unable to retrieve last oplog. Maybe you are not running your mongodb in a replica set");
    }

    public Optional<BsonTimestamp> getLastKnown() {
        MongoCollection<Document> oplogOffset = database.getCollection("mongooplog");
        Document lastProcessedOplog = oplogOffset.find(eq("_id", identifier)).first();
        Optional<BsonTimestamp> checkpoint = Optional.empty();
        if (lastProcessedOplog != null && lastProcessedOplog.get("ts", BsonTimestamp.class) != null) {
            checkpoint = Optional.ofNullable(lastProcessedOplog.get("ts", BsonTimestamp.class));
            log.info("Checkpoint found in the administrative database : {}.", checkpoint.get().toString());
        }
        return checkpoint;
    }


    public void keep(BsonTimestamp timestamp) {
        MongoCollection<Document> collection = database.getCollection("mongooplog");
        collection.updateOne(eq("_id", identifier), set("ts", timestamp), new UpdateOptions().upsert(true));
    }

    public void storeImportStart() {
        MongoCollection<Document> collection = database.getCollection("mongooplog");
        collection.updateOne(eq("_id", identifier), combine(
                set("import", "running"),
                set("start", new Date())
        ), new UpdateOptions().upsert(true));
    }

    public void storeImportEnd(float length) {
        float lenghtInMinutes = (length/1000F)/60F;

        MongoCollection<Document> collection = database.getCollection("mongooplog");
        collection.updateOne(eq("_id", identifier), combine(
                set("import", "done"),
                set("end", new Date()),
                set("length", lenghtInMinutes)
                )
                , new UpdateOptions().upsert(true));

    }

    public InitialImport lastImportStatus() {
        MongoCollection<Document> collection = database.getCollection("mongooplog");
        Document status = collection.find(eq("_id", identifier)).first();
        Date start = status.getDate("start");
        Date end = status.getDate("end");
        String state = status.getString("import");
        Double length = status.getDouble("length");

        InitialImport initialImport = new InitialImport();
        initialImport.setStart(start);
        initialImport.setEnd(end);
        initialImport.setStatus(state);
        initialImport.setLengthInMinutes(length);
        return initialImport;
    }
}

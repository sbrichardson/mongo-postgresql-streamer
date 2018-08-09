package com.malt.mongopostgresqlstreamer;

import com.malt.mongopostgresqlstreamer.connectors.Connector;
import com.malt.mongopostgresqlstreamer.model.FlattenMongoDocument;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

import static com.mongodb.client.model.Filters.*;

@Service
public class OplogStreamer {

    @Value(value = "${mongo.connector.identifier:test}")
    private String identifier;

    @Value(value = "${mongo.database:test}")
    private String dbName;

    @Autowired
    private MongoClient client;
    @Autowired
    private MappingsManager mappingsManager;
    @Autowired
    private OplogStreamer oplogReader;
    @Autowired
    private CheckpointManager checkpointManager;
    @Autowired
    @Qualifier("oplog")
    private MongoDatabase oplog;
    @Autowired
    @Qualifier("database")
    private MongoDatabase database;
    @Autowired
    private List<Connector> connectors;

    public void watchFromCheckpoint(Optional<BsonTimestamp> checkpoint) {
        for (Document document : oplog.getCollection("oplog.rs").find(oplogfilters(checkpoint)).cursorType(CursorType.TailableAwait)) {
            BsonTimestamp timestamp = processOperation(document);
            checkpointManager.keep(timestamp);
        }
    }

    @Transactional
    BsonTimestamp processOperation(Document document) {
        Long id = document.getLong("h");
        String namespace = document.getString("ns");
        String collection = namespace.split("\\.")[1];
        String operation = document.getString("op");
        BsonTimestamp timestamp = document.get("ts", BsonTimestamp.class);

        switch (operation) {
            case "i":
                connectors.forEach(connector ->
                        connector.insert(
                                collection,
                                FlattenMongoDocument.fromMap(document),
                                mappingsManager.mappingConfigs.getDatabaseMappings().get(0)
                        )
                );
                break;
            case "u":
                connectors.forEach(connector ->
                        connector.update(
                                collection,
                                FlattenMongoDocument.fromMap(document),
                                mappingsManager.mappingConfigs.getDatabaseMappings().get(0)
                        )
                );
                break;
            case "d":
                connectors.forEach(connector ->
                        connector.remove(
                                collection,
                                FlattenMongoDocument.fromMap(document),
                                mappingsManager.mappingConfigs.getDatabaseMappings().get(0)
                        )
                );
                break;
            default:
                break;
        }


        return timestamp;
    }

    private void remove(Document document) {
        Object id = document.get("o");
        System.out.println("Remove document by id : " + id);
    }

    private void update(Document document) {

    }

    private void insert(Document document) {
        Object doc = document.get("o");
        System.out.println("Insert new doc : " + doc);
    }


    private Bson oplogfilters(Optional<BsonTimestamp> checkpoint) {
        if (checkpoint.isPresent()) {
            return and(
                    gt("ts", checkpoint.get()),
                    ne("ns", database.getName() + ".mongooplog"),
                    regex("ns", database.getName()));
        }
        return and(
                ne("ns", database.getName() + ".mongooplog"),
                regex("ns", database.getName()));
    }


}

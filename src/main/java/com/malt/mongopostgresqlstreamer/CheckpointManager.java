package com.malt.mongopostgresqlstreamer;

import com.malt.mongopostgresqlstreamer.model.Mappings;
import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.swing.text.html.Option;
import java.io.IOException;
import java.util.Optional;

import static com.mongodb.client.model.Filters.*;

@Service
public class CheckpointManager {

    @Value(value = "${mongo.connector.identifier:test}")
    private String identifier;

    @Autowired
    @Qualifier("database")
    private MongoDatabase database;

    public Optional<BsonTimestamp> getLastKnown() {
        MongoCollection<Document> oplogOffset = database.getCollection("mongooplog");
        Document lastProcessedOplog = oplogOffset.find(eq("_id", identifier)).first();
        Optional<BsonTimestamp> checkpoint = Optional.empty();
        if (lastProcessedOplog != null) {
            checkpoint = Optional.ofNullable(lastProcessedOplog.get("ts", BsonTimestamp.class));
        }
        return checkpoint;
    }


    public void keep(BsonTimestamp timestamp) {
        MongoCollection<Document> collection = database.getCollection("mongooplog");
        Document offset = new Document("_id", identifier);
        offset.append("ts", timestamp);
        Object id = offset.get("_id");
        if (id == null) {
            collection.insertOne(offset);
        } else {
            collection.replaceOne(eq("_id", id), offset, new UpdateOptions().upsert(true));
        }
    }
}

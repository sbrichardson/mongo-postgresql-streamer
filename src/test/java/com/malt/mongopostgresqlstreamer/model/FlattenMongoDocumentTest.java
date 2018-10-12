package com.malt.mongopostgresqlstreamer.model;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.*;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class FlattenMongoDocumentTest {

    @Test
    void id_Field_As_ObjectId_is_Correctly_Mapped_And_Generate_A_CreationDate() {
        Document document = new Document();
        document.put("_id", new ObjectId());

        FlattenMongoDocument flattenMongoDocument = FlattenMongoDocument.fromDocument(document);
        assertThat(flattenMongoDocument.get("_creationdate")).isPresent();
        assertThat(flattenMongoDocument.get("_creationdate").get())
                .isNotNull()
                .isInstanceOf(Date.class);

        flattenMongoDocument = FlattenMongoDocument.fromMap(document);
        assertThat(flattenMongoDocument.get("_creationdate")).isPresent();
        assertThat(flattenMongoDocument.get("_creationdate").get())
                .isNotNull()
                .isInstanceOf(Date.class);

    }

    @Test
    void id_Field_As_String_is_Correctly_Mapped_And_Generate_A_CreationDate() {
        Document document = new Document();
        document.put("_id", new ObjectId().toString());

        FlattenMongoDocument flattenMongoDocument = FlattenMongoDocument.fromDocument(document);
        assertThat(flattenMongoDocument.get("_creationdate")).isPresent();
        assertThat(flattenMongoDocument.get("_creationdate").get())
                .isNotNull()
                .isInstanceOf(Date.class);

        flattenMongoDocument = FlattenMongoDocument.fromMap(document);
        assertThat(flattenMongoDocument.get("_creationdate")).isPresent();
        assertThat(flattenMongoDocument.get("_creationdate").get())
                .isNotNull()
                .isInstanceOf(Date.class);
    }

    @Test
    void id_Field_not_object_id_is_Correctly_Mapped_And_Generate_no_errors() {
        Document document = new Document();
        document.put("_id", "anything");

        FlattenMongoDocument flattenMongoDocument = FlattenMongoDocument.fromDocument(document);
        assertThat(flattenMongoDocument.get("_creationdate")).isNotPresent();

        flattenMongoDocument = FlattenMongoDocument.fromMap(document);
        assertThat(flattenMongoDocument.get("_creationdate")).isNotPresent();
    }

    @Test
    void date_Field_correctly_Mapped() {
        Document document = new Document();
        document.put("_id", "anything");
        document.put("date", new Date());

        FlattenMongoDocument flattenMongoDocument = FlattenMongoDocument.fromDocument(document);
        assertThat(flattenMongoDocument.get("date")).isPresent();
        assertThat(flattenMongoDocument.get("date").get())
                .isNotNull()
                .isInstanceOf(Date.class);


        document.put("anotherdate.$date", new Date().getTime());
        flattenMongoDocument = FlattenMongoDocument.fromMap(document);
        assertThat(flattenMongoDocument.get("date")).isPresent();
        assertThat(flattenMongoDocument.get("date").get())
                .isNotNull()
                .isInstanceOf(Date.class);
        assertThat(flattenMongoDocument.get("anotherdate")).isPresent();
        assertThat(flattenMongoDocument.get("anotherdate").get())
                .isNotNull()
                .isInstanceOf(Date.class);
    }

    @Test
    void numbers_are_Correctly_Mapped() {
        Document document = new Document();
        document.put("long", 500L);
        document.put("bigdecimal", BigDecimal.TEN);
        document.put("integer", 200);

        FlattenMongoDocument flattenMongoDocument = FlattenMongoDocument.fromDocument(document);
        assertThat(flattenMongoDocument.get("bigdecimal").get()).isEqualTo(BigDecimal.TEN);
        assertThat(flattenMongoDocument.get("long").get()).isEqualTo(500L);
        assertThat(flattenMongoDocument.get("integer").get()).isEqualTo(new BigDecimal(200));

        flattenMongoDocument = FlattenMongoDocument.fromMap(document);
        assertThat(flattenMongoDocument.get("bigdecimal").get()).isEqualTo(BigDecimal.TEN);
        assertThat(flattenMongoDocument.get("long").get()).isEqualTo(500L);
        assertThat(flattenMongoDocument.get("integer").get()).isEqualTo(200);
    }

    @Test
    @SuppressWarnings("unchecked")
    void it_should_not_flatten_arrays() {
        Document teamDocument = new Document();
        teamDocument.put("id", new ObjectId().toHexString());
        teamDocument.put("name", "The Avengers");
        teamDocument.put("members", asList(
                givenTeamMemberDocument("Hulk"),
                givenTeamMemberDocument("Iron Man")
        ));

        FlattenMongoDocument flattenedDocument = FlattenMongoDocument.fromDocument(teamDocument);

        assertThat(flattenedDocument).isNotNull();
        assertThat(flattenedDocument.getValues()).hasSize(3).contains(
                entry("id", teamDocument.get("id")),
                entry("name", teamDocument.get("name"))
        );

        List<Map<String, Object>> members = (List<Map<String, Object>>) flattenedDocument.getValues().get("members");
        assertThat(members).hasSize(2);
        assertThat(members.get(0).get("name")).isEqualTo("Hulk");
        assertThat(members.get(1).get("name")).isEqualTo("Iron Man");
    }

    private static Document givenTeamMemberDocument(String name) {
        Document document = new Document();
        document.put("id", new ObjectId().toHexString());
        document.put("name", name);
        return document;
    }
}
package com.malt.mongopostgresqlstreamer.model;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

public class FlattenMongoDocumentTest {

    @Test
    public void id_Field_As_ObjectId_is_Correctly_Mapped_And_Generate_A_CreationDate() {
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
    public void id_Field_As_String_is_Correctly_Mapped_And_Generate_A_CreationDate() {
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
    public void id_Field_not_object_id_is_Correctly_Mapped_And_Generate_no_errors() {
        Document document = new Document();
        document.put("_id", "anything");

        FlattenMongoDocument flattenMongoDocument = FlattenMongoDocument.fromDocument(document);
        assertThat(flattenMongoDocument.get("_creationdate")).isNotPresent();

        flattenMongoDocument = FlattenMongoDocument.fromMap(document);
        assertThat(flattenMongoDocument.get("_creationdate")).isNotPresent();
    }

    @Test
    public void date_Field_correctly_Mapped() {
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
    public void numbers_are_Correctly_Mapped() {
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
}
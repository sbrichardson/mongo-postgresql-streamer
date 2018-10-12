package com.malt.mongopostgresqlstreamer.connectors.postgres.batch;

import com.malt.mongopostgresqlstreamer.connectors.postgres.Field;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class SingleTableCopyOperationsTest {

    @Test
    void it_should_fail_if_there_are_more_fields_than_expected() {
        String tableName = "users";
        List<FieldMapping> expectedFields = asList(
                givenFieldMapping("INT","id"),
                givenFieldMapping("TEXT","first_name"),
                givenFieldMapping("TEXT","last_name")
        );

        CopyManager copyManager = mock(CopyManager.class);
        SingleTableCopyOperations operations = new SingleTableCopyOperations(tableName, expectedFields, copyManager);

        List<Field> fields = asList(
                givenField("id", 1L),
                givenField("first_name", "John"),
                givenField("last_name", "Doe"),
                givenField("birthday", "01/01/2000")
        );

        assertThatThrownBy(() -> operations.addOperation(fields))
                .isExactlyInstanceOf(InvalidTableFieldException.class)
                .hasMessage("Operations to table 'users' expects 3 values (first_name, id, last_name) but received 4 (id, first_name, last_name, birthday)");
    }

    private static FieldMapping givenFieldMapping(String type, String name) {
        return new FieldMapping("users", name, type, false, null, name);
    }

    private static Field givenField(String name, Object value) {
        return new Field(name, value);
    }
}

package com.malt.mongopostgresqlstreamer.connectors.postgres;

import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

@Service
@Slf4j
public class SqlExecutor {
    private final JdbcTemplate jdbcTemplate;

    @Inject
    public SqlExecutor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    void update(String table, List<Field> fields, String primaryKey, Object primaryKeyValue) {
        String query = format(
                "UPDATE %s SET %s WHERE %s = ?",
                table, generateUpdateString(fields), primaryKey
        );

        List<Object> values = getValues(fields);
        values.add(primaryKeyValue);
        Object[] valuesArray = values.toArray();

        log.trace("{} {}", query, valuesArray);
        jdbcTemplate.update(query, valuesArray);
    }

    void upsert(String table, String primaryKey, List<Field> fields) {
        String commaSeparatedFieldNames = getCommaSeparatedFieldNames(fields);
        String placeholders = getPlaceholders(fields);
        String query = format(
                "INSERT INTO %s (%s) VALUES(%s) ON CONFLICT (%s) DO UPDATE SET %s",
                table, commaSeparatedFieldNames, placeholders,
                primaryKey, generateUpdateString(fields)
        );

        List<Object> values = getValues(fields);
        values.addAll(values); // Duplicates are needed for the UPDATE clause
        Object[] valuesArray = values.toArray();

        log.trace("{} {}", query, valuesArray);
        jdbcTemplate.update(query, valuesArray);
    }

    void dropTable(String table) {
        log.debug("Dropping table '{}'...", table);
        sqlExecute("DROP TABLE IF EXISTS %s", table);
    }

    void addPrimaryKey(String table, String primaryKeyName) {
        log.debug("Adding primary key '{}' on table '{}' ...", primaryKeyName, table);
        sqlExecute("ALTER TABLE %s ADD PRIMARY KEY(%s)", table, primaryKeyName);
    }

    void createTable(String table, List<FieldMapping> fieldMappings) {
        String formattedFields = fieldAndTypes(fieldMappings);
        log.debug("Creating table '{}' with fields {}", table, formattedFields);
        sqlExecute("CREATE TABLE %s (%s)", table, formattedFields);
    }

    void createIndex(String index) {
        log.debug("Creating index '{}'", index);
        sqlExecute("CREATE " + index);
    }

    void remove(String table, String primaryKey, Object primaryKeyValue) {
        log.debug("Remove document where '{} = {}' from {}", primaryKey, primaryKeyValue, table);
        jdbcTemplate.update(
                format("DELETE FROM %s WHERE %s = ? ", table, primaryKey),
                new Object[]{primaryKeyValue}
        );
    }

    private void sqlExecute(String query, Object... parameters) {
        String sql = format(query, parameters);

        log.trace(sql);
        jdbcTemplate.execute(sql);
    }


    private List<Object> getValues(List<Field> fields) {
        return fields.stream()
                .map(Field::getValue)
                .collect(toList());
    }

    private String getPlaceholders(List<Field> fields) {
        return String.join(
                ", ",
                fields.stream().map(p -> "?").collect(toList())
        );
    }

    private String getCommaSeparatedFieldNames(List<Field> fields) {
        return String.join(
                ", ",
                fields.stream().map(Field::getName).collect(toList())
        );
    }

    private String fieldAndTypes(List<FieldMapping> fieldMappings) {
        return fieldMappings
                .stream()
                .filter(f -> !f.getType().startsWith("_"))
                .map(f -> f.getDestinationName() + " " + f.getType())
                .collect(Collectors.joining(","));
    }

    private String generateUpdateString(List<Field> fields) {
        return String.join(
                ", ",
                fields.stream()
                        .map(field -> field.getName() + " = ?")
                        .collect(toList())
        );
    }
}

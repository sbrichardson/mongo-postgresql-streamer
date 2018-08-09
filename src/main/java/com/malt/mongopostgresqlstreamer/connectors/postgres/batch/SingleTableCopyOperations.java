package com.malt.mongopostgresqlstreamer.connectors.postgres.batch;

import com.malt.mongopostgresqlstreamer.connectors.postgres.Field;
import com.malt.mongopostgresqlstreamer.model.FieldMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;

@Slf4j
class SingleTableCopyOperations {
    private static final Object lock = new Object();

    private final int maxElementsBeforeCallback;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicInteger valueCounter = new AtomicInteger();

    private final Consumer<SingleTableCopyOperations> callback;

    private final String table;
    private final String fieldsHeader;
    private final List<String> fieldNames;

    private String copyString;

    SingleTableCopyOperations(
            String table, List<FieldMapping> fields,
            int maxSecondsBeforeCallback, int maxElementsBeforeCallback,
            Consumer<SingleTableCopyOperations> callback
    ) {
        this.table = table;
        this.fieldNames = fields.stream().filter(mapping -> !mapping.isAnArray()).map(FieldMapping::getDestinationName).sorted().collect(toList());
        this.fieldsHeader = serialize(fieldNames);
        this.copyString = fieldsHeader;

        this.maxElementsBeforeCallback = maxElementsBeforeCallback;
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
        this.scheduledExecutorService.scheduleAtFixedRate(
                this::releaseValues, maxSecondsBeforeCallback, maxSecondsBeforeCallback, TimeUnit.SECONDS
        );

        this.callback = callback;
    }

    void addOperation(List<Field> fields) {
        if (fields.size() < fieldNames.size()) {
            // Add missing field with NULL value
            log.trace(
                    "Expecting {} values but received {}. Adding NULL values to complete missing ones.",
                    fieldNames.size(),
                    fields.size()
            );
            getMissingFields(fields)
                    .forEach(f -> fields.add(new Field(f, null)));
        } else if (fields.size() > fieldNames.size()) {
            throw new RuntimeException("Expecting " + fieldNames.size() + " values but received " + fields.size());
        }

        synchronized (lock) {
            this.copyString += serialize(fields.stream().sorted().map(Field::getValue).collect(toList()));
            this.valueCounter.incrementAndGet();

            if (valueCounter.get() >= maxElementsBeforeCallback) {
                releaseValues();
            }
        }
    }

    private List<String> getMissingFields(List<Field> fields) {
        List<String> missingFields = new ArrayList<>();
        fieldNames.forEach(fieldName -> {
            if (!fields.stream().anyMatch(f -> f.getName().equals(fieldName))) {
                missingFields.add(fieldName);
            }
        });

        return missingFields;
    }

    private String serialize(List<?> parameters) {
        return String.join(
                ",",
                parameters.stream()
                        .map(this::serializeObject)
                        .collect(toList())
        ) + "\n";
    }

    private String serializeObject(Object parameter) {
        // TODO: Complete with other types ?

        if (parameter == null) {
            return "null";
        }

        if (parameter instanceof Boolean) {
            return (boolean) parameter ? "TRUE" : "FALSE";
        }

        // For unknown reason, the Mongo driver serialize some Int32 values
        // into BigDecimal.
        // Obviously, COPY expect a int (and not a double) which means
        // I have to check for this corner case
        if (parameter instanceof BigDecimal) {
            if (((BigDecimal) parameter).intValue() == ((BigDecimal) parameter).doubleValue()) {
                return String.valueOf(((BigDecimal) parameter).intValueExact());
            }
        }

        return StringEscapeUtils.escapeCsv(
                parameter.toString()
                        // PostgreSql does not like UTF8 NULL byte or
                        .replace("\u0000", "")
        );
    }

    private void releaseValues() {
        synchronized (lock) {
            if (valueCounter.get() != 0) {
                callback.accept(this);
                this.copyString = fieldsHeader;
            }

            valueCounter.set(0);
        }
    }

    String getTable() {
        return table;
    }

    String getCopyContent() {
        return copyString;
    }

    InputStream getCopyContentStream() {
        return new ByteArrayInputStream(getCopyContent().getBytes());
    }
}

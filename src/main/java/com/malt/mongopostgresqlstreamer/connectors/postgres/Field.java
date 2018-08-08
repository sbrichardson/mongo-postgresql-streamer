package com.malt.mongopostgresqlstreamer.connectors.postgres;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Field {
    private String name;
    private Object value;

    public boolean isList() {
        return value instanceof Iterable;
    }
}

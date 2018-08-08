package com.malt.mongopostgresqlstreamer.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldMapping {

    private String sourceName;
    private String destinationName;

    private String type;
    private boolean indexed;

    private String foreignKey;

    private String scalarFieldDestinationName;
}

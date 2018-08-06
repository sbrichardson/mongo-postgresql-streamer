package com.malt.mongopostgresqlstreamer.model;

import lombok.Data;

import java.util.List;

@Data
public class Table {

    private String pk;
    private List<Field> fields;
    private String name;
}

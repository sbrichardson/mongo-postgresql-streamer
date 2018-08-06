package com.malt.mongopostgresqlstreamer.model;

import lombok.Data;

import java.util.List;

@Data
public class Database {

    private String name;
    private List<Table> tables;
}

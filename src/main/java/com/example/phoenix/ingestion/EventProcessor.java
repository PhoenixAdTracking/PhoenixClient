package com.example.phoenix.ingestion;

import lombok.AllArgsConstructor;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 * Class for processing ad events.
 */
@AllArgsConstructor
public class EventProcessor {

    private final BasicDataSource phoenixDb;

    public int storeEvent;

}

package com.springapp.batch;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Partitioner that creates as many partitions as the gridSize.
 */
public class MesaPartitioner implements Partitioner {

    /** key for int */
    public static final String TABLE_NUM_KEY = "tableNum";
    /** key for List<String> that is intended to house the dataSource toString values by the reader and writer */
    public static final String DATASOURCE_LIST_KEY_PREFIX = "listKey";

    @Override
    public Map<String, ExecutionContext> partition(final int gridSize) {

        final Map<String,ExecutionContext> partitions = new HashMap<>();
        for (int i=1; i <= gridSize; i++) {
            final ExecutionContext context = new ExecutionContext();
            context.put(TABLE_NUM_KEY, i);
            context.put(DATASOURCE_LIST_KEY_PREFIX+i, new ArrayList<String>());
            partitions.put("step" + i, context);
        }
        return partitions;
    }
}

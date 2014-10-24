package com.springapp.batch;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;

import javax.sql.DataSource;
import java.util.List;

import static com.springapp.batch.MesaPartitioner.DATASOURCE_LIST_KEY_PREFIX;
import static com.springapp.batch.MesaPartitioner.TABLE_NUM_KEY;

public class SpyUtils {

    public static void saveDataSourceToString(StepExecution stepExecution, DataSource dataSource) {
        final int partitionNum = (int) stepExecution.getExecutionContext().get(TABLE_NUM_KEY);
        final String listKeyForPartition = DATASOURCE_LIST_KEY_PREFIX + partitionNum;
        final List<String> dataSourcesUsed =
                (List<String>) stepExecution.getExecutionContext().get(listKeyForPartition);
        dataSourcesUsed.add(dataSource.toString());

        // promote list for access after job ends via jobExecution
        final ExecutionContext jec = stepExecution.getJobExecution().getExecutionContext();
        jec.put(listKeyForPartition, dataSourcesUsed);
    }
}

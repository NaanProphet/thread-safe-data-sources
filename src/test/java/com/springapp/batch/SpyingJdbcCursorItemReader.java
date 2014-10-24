package com.springapp.batch;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.util.Assert;

import static com.springapp.batch.SpyUtils.saveDataSourceToString;

public class SpyingJdbcCursorItemReader<T> extends JdbcCursorItemReader<T> implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        Assert.isTrue(isUseSharedExtendedConnection(),
                "Incorrect job setup! Reader must have set isUseSharedExtendedConnection to true");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        saveDataSourceToString(stepExecution, getDataSource());
        return null;
    }
}

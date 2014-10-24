package com.springapp.batch;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.util.List;

import static com.springapp.batch.MesaPartitioner.DATASOURCE_LIST_KEY_PREFIX;
import static com.springapp.batch.MesaPartitioner.TABLE_NUM_KEY;
import static com.springapp.batch.SpyUtils.saveDataSourceToString;

public class SpyingJdbcBatchItemWriter<T> extends JdbcBatchItemWriter<T> implements StepExecutionListener {

    private DataSource dataSource;

    @Override
    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
        super.setDataSource(dataSource);
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(dataSource, "Hello user! You must set the data source via the property argument for the spying to work!");
        super.afterPropertiesSet();
    }


    @Override
    public void beforeStep(StepExecution stepExecution) {
        // intentionally blank
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        saveDataSourceToString(stepExecution, dataSource);
        return null;
    }

}

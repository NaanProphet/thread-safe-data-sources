package com.springapp.batch;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.database.ExtendedConnectionDataSourceProxy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.jdbc.JdbcTestUtils;

import javax.sql.DataSource;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.springapp.batch.MesaPartitioner.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:spring-config.xml")
@DirtiesContext
public class ExtendedConnectionDataSourceProxyFactoryTest {

    /**
     * The number of partitions to create.
     */
    public static final int GRID_SIZE = 10;

    /**
     * Should be used for both corePoolSize and maxPoolSize
     */
    public static final int POOL_SIZE = 3;

    /**
     * template SQL create table/insert script
     */
    public static final String TESTDATA_TEMPLATE_SQL = "testdata_template.sql";

    /**
     * placeholder table name to be replaced in template sql script
     */
    public static final String SQL_TEMPLATE_TABLE_NAME_PLACEHOLDER = "REF_TABLE_X";

    /**
     * prefix of dummy table name, sans the number (e.g. REF_TABLE_ for REF_TABLE_1)
     */
    public static final String REF_TABLE_PREFIX = "REF_TABLE_";

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job job;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static JobExecution jobExecution;

    private static AtomicBoolean isLaunchJob = new AtomicBoolean(true);

    // -------------- SETUP AND KICK OFF JOB (ONCE) -----------------------------

    @Before
    public void setup() throws Exception {
        // only launch job once. forces stateful static fields ... open to ideas
        if (isLaunchJob.getAndSet(false)) {
            assertThat("Silly billy! The purpose is to test the thread safety of the ExtendedConnectionDataSourceProxy! Check setup",
                    dataSource, instanceOf(ExtendedConnectionDataSourceProxy.class));

            createSqlTestData();

            JobParametersBuilder builder = new JobParametersBuilder();
            jobExecution = jobLauncher.run(job, builder.toJobParameters());
            assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
        }
    }

    /**
     * Creates a new table (with test data) for each partition (using GRID_SIZE).
     */
    private void createSqlTestData() throws Exception {
        final File templateSql = new ClassPathResource(TESTDATA_TEMPLATE_SQL).getFile();
        for (int i = 1; i <= GRID_SIZE; i++) {
            final File ithSqlFile = temporaryFolder.newFile("sqlInsert" + i + ".sql");
            final String sqlTemplate = FileUtils.readFileToString(templateSql);
            final String ithSqlInsert = sqlTemplate.replaceAll(SQL_TEMPLATE_TABLE_NAME_PLACEHOLDER, REF_TABLE_PREFIX + i);
            FileUtils.writeStringToFile(ithSqlFile, ithSqlInsert);
            final Resource ithSqlResource = new FileSystemResource(ithSqlFile);
            final boolean continueOnError = false;
            JdbcTestUtils.executeSqlScript(jdbcTemplate, ithSqlResource, continueOnError);
        }
    }

    // -------------- PRELIM TESTS, ENSURE JOB FUNCTIONALITY --------------

    /**
     * Tests that the job is correctly setup to promote objects required for
     * asserting threading logic are present in the jobExecutionContext returned from the test.
     */
    @Test
    public void testJobExecutionPromotion() throws Exception {
        for (int i = 1; i <= GRID_SIZE; i++) {
            final List<String> dataSourceToStrings =
                    (List<String>) jobExecution.getExecutionContext().get(DATASOURCE_LIST_KEY_PREFIX + i);
            assertThat("size should be two, one for reader and one for writer", dataSourceToStrings.size(), is(2));
        }
    }

    /**
     * Tests that the toString generated by the dataSource used by the job lists its memory
     * address at the end. Otherwise, subsequent tests will not be able to verify if different
     * threads received different dataSource objects.
     */
    @Test
    public void testDataSourceToStringsContainsMemoryAddress() throws Exception {
        final List<String> allToStrings = new ArrayList<>();
        for (int i = 1; i <= GRID_SIZE; i++) {
            final List<String> dataSourceToStrings =
                    (List<String>) jobExecution.getExecutionContext().get(DATASOURCE_LIST_KEY_PREFIX + i);
            allToStrings.addAll(dataSourceToStrings);
        }

        for (String toString : allToStrings) {
            assertThat("expected memory address at end of dataSource's toString: " + toString,
                    toString.matches("^.*ExtendedConnectionDataSourceProxy@(\\d|[a-f])+$"), is(true));
        }
    }

    // -------------- ACTUAL THREADING TESTS OF PROXY DATA SOURCE --------------

    /**
     * Verifies that the reader and writer of the same step receive the same dataSource proxy object.
     */
    @Test
    public void testReadersAndWritersReceivedSameDataSource() throws Exception {
        for (int i = 1; i <= GRID_SIZE; i++) {
            final List<String> dataSourceToStrings =
                    (List<String>) jobExecution.getExecutionContext().get(DATASOURCE_LIST_KEY_PREFIX + i);
            assertThat("reader and writer didn't receive same data source, oh my!",
                    dataSourceToStrings.get(0),
                    is(dataSourceToStrings.get(1)));
        }
    }

    /**
     * Verfies that concurrently running partitioned steps received different proxied
     * dataSource objects based on pool size, thus checking if data sources were recycled by thread.
     */
    @Test
    public void testDifferentThreadsGotDifferentDataSources() throws Exception {
        final Set<String> uniqueDataSources = new HashSet<>();
        for (int i = 1; i <= GRID_SIZE; i++) {
            List<String> dataSourceToStrings =
                    (List<String>) jobExecution.getExecutionContext().get(DATASOURCE_LIST_KEY_PREFIX + i);
            uniqueDataSources.addAll(dataSourceToStrings);
        }
        // use Math.min in case number of partitions is less than threadPoolSize
        assertThat("expected each thread in the thread pool to receive a different dataSource from the factory!",
                uniqueDataSources.size(), is(equalTo(Math.min(GRID_SIZE, POOL_SIZE))));
    }

}

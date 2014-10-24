package com.springapp.batch;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.ExtendedConnectionDataSourceProxy;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.text.SimpleDateFormat;

/**
 * Factory bean for returning a thread-safe version of the otherwise not thread-safe {@link ExtendedConnectionDataSourceProxy}.
 * The {@link DataSource} returned by this factory is intended to prevent deadlocks between
 * JDBC readers and JDBC writers that modify the same rows that are read in the same chunk
 * (e.g. when using a {@link org.springframework.batch.item.database.JdbcCursorItemReader} and
 * {@link org.springframework.batch.item.database.JdbcBatchItemWriter}).
 * <p>
 *     To use this factory, <ol>
 *         <li>register the factory as a <b>singleton bean</b> with the respective <code>originalDataSource</code> to be proxied
 *     <pre>
 *          &lt;bean id="dataSource" class="com.springapp.batch.ExtendedConnectionDataSourceProxyFactory"&gt;
 *               &lt;constructor-arg name="dataSource" ref="originalDataSource"/&gt;
 *          &lt;/bean&gt;
 *     </pre>
 *         </li>
 *         <li>set the reader's <code>useSharedExtendedConnection</code> property to <code>true</code> and refer
 *         to the proxied <code>dataSource</code> in both the reader and writer. Note: failure to specify both beans as
 *         <b>step scope</b> can cause threading issues (e.g. in concurrently running partitioned steps)
 *     <pre>
 *        &lt;bean name="cursorItemReader" class="org.springframework.batch.item.database.JdbcCursorItemReader" scope="step"&gt;
 *           &lt;property name="dataSource" ref="dataSource"/&gt;
 *           &lt;property name="useSharedExtendedConnection" value="true"/&gt;
 *           ...
 *        &lt;/bean&gt;
 *
 *        &lt;bean id="batchItemWriter" class="org.springframework.batch.item.database.JdbcBatchItemWriter" scope="step"&gt;
 *            &lt;property name="dataSource" ref="dataSource"/&gt;
 *            ...
 *        &lt;/bean&gt;
 *     </pre>
 *         </li>
 *     </ol>
 * </p>
 *
 */
public class ExtendedConnectionDataSourceProxyFactory implements FactoryBean<ExtendedConnectionDataSourceProxy> {

    private static Logger LOGGER = LoggerFactory.getLogger(ExtendedConnectionDataSourceProxyFactory.class);

    private final DataSource dataSource;

    /**
     * @param dataSource the original dataSource to be wrapped in a thread-safe proxy
     */
    public ExtendedConnectionDataSourceProxyFactory(final DataSource dataSource) {
        Assert.notNull(dataSource, "Silly application context, dataSources cannot be null!");
        this.dataSource = dataSource;
    }

    private final ThreadLocal<ExtendedConnectionDataSourceProxy> threadSafeDataSource =
            new ThreadLocal<ExtendedConnectionDataSourceProxy>() {
                @Override
                public ExtendedConnectionDataSourceProxy initialValue() {
                    return new ExtendedConnectionDataSourceProxy(dataSource);
                }
            };


    @Override
    public ExtendedConnectionDataSourceProxy getObject() throws Exception {
        final ExtendedConnectionDataSourceProxy returnMe = threadSafeDataSource.get();
        LOGGER.info("Returning thread-safe proxy: [{}]", returnMe);
        return returnMe;
    }

    @Override
    public Class<?> getObjectType() {
        return ExtendedConnectionDataSourceProxy.class;
    }

    @Override
    public boolean isSingleton() {
        // tell Spring never to cache the object returned by this factory, as it will change from thread to thread
        return false;
    }

}

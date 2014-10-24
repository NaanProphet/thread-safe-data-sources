package com.springapp.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class SimpleProcessor implements ItemProcessor<String, SimpleItem> {

    private static Logger LOGGER = LoggerFactory.getLogger(SimpleProcessor.class);

    @Override
    public SimpleItem process(String item) throws Exception {
        LOGGER.info(item);
        return new SimpleItem(item);
    }
}

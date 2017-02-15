package com.ldbc.driver;

import com.ldbc.driver.runtime.coordination.UpdatesProducer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class DbConnectionState implements Closeable {
    private UpdatesProducer updateProducer = null;
    private static final String KAFKA_PRODUCER_PROPERTIES = "producer.properties";

    public UpdatesProducer getUpdateProducer() {
        return updateProducer;
    }

    public void setUpKafka() throws DbException {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            input = classLoader.getResourceAsStream( KAFKA_PRODUCER_PROPERTIES );
            prop.load( input );
        } catch (IOException ex) {
            throw new DbException("Kafka Producer could NOT be instantiated", ex);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    throw new DbException("Kafka Producer could NOT be instantiated", e);
                }
            }
        }
        updateProducer = new UpdatesProducer( new KafkaProducer<String, Operation>( prop ) );
    }
}

package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

class ConsumerOperationStreamExecutorServiceThread extends Thread {
    private static final long POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI = 100;
    private static final String KAFKA_CONSUMER_PROPERTIES = "consumer.properties";

    private final OperationExecutor operationExecutor;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private KafkaConsumer<String, Operation> consumer;

    public ConsumerOperationStreamExecutorServiceThread(
        OperationExecutor operationExecutor,
        ConcurrentErrorReporter errorReporter,
        AtomicBoolean hasFinished,
        AtomicBoolean forcedTerminate ) {
        super( ConsumerOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis() );
        this.operationExecutor = operationExecutor;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        setUpKafka();
    }

    public void setUpKafka() {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            input = classLoader.getResourceAsStream( KAFKA_CONSUMER_PROPERTIES );
            prop.load( input );
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            consumer = new KafkaConsumer<>( prop );
        } catch (Exception e) {
            e.printStackTrace();
            consumer = null;
        }
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, Operation> records = consumer.poll( 0 );
                for (ConsumerRecord<String, Operation> record : records) {
                    Operation operation = record.value();
                    operationExecutor.execute( operation );
                }
            }
        } catch (OperationExecutorException e) {
            errorReporter.reportError( this, ConcurrentErrorReporter.stackTraceToString( e ) );
        }
       // finally {
       //     while (0 < operationExecutor.uncompletedOperationHandlerCount() && !forcedTerminate.get()) {
       //         Spinner.powerNap( POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI );
       //     }
       //     this.hasFinished.set( true );
       // }
    }
}
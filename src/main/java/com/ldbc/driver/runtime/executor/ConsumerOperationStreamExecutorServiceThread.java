package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

class ConsumerOperationStreamExecutorServiceThread extends Thread
{
    private static final long POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI = 100;
    private static final String KAFKA_CONSUMER_PROPERTIES = "consumer.properties";
    private static final String TOPIC = "ldbc_updates";


    private final OperationExecutor operationExecutor;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private KafkaConsumer<String, Operation> consumer;

    public ConsumerOperationStreamExecutorServiceThread(
            OperationExecutor operationExecutor,
            ConcurrentErrorReporter errorReporter,
            AtomicBoolean hasFinished,
            AtomicBoolean forcedTerminate ) throws OperationExecutorException
    {
        super( ConsumerOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis() );
        this.operationExecutor = operationExecutor;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        try
        {
            setUpKafka();
        }
        catch ( OperationExecutorException e )
        {
            throw new OperationExecutorException( "Kafka Consumer could NOT be instantiated", e );
        }
    }

    public void setUpKafka() throws OperationExecutorException
    {
        Properties prop = new Properties();
        InputStream input = null;
        try
        {
            ClassLoader classLoader = getClass().getClassLoader();
            input = classLoader.getResourceAsStream( KAFKA_CONSUMER_PROPERTIES );
            prop.load( input );
            prop.setProperty( "group.id", UUID.randomUUID().toString() );
        }
        catch ( IOException ex )
        {
            throw new OperationExecutorException( "Kafka consumer configuration could not be retrieved", ex );
        }
        finally
        {
            if ( input != null )
            {
                try
                {
                    input.close();
                }
                catch ( IOException e )
                {
                    throw new OperationExecutorException( "Kafka consumer configuration could not be retrieved", e );
                }
            }
        }
        try
        {
            consumer = new KafkaConsumer<>( prop );
            consumer.subscribe( Collections.singletonList( TOPIC ) );
        }
        catch ( Exception e )
        {
            consumer = null;
            throw new OperationExecutorException( "KafkaConsumer could NOT be instantiated", e );
        }
    }

    @Override
    public void run()
    {
        try
        {
            while ( !forcedTerminate.get() )
            {
                ConsumerRecords<String, Operation> records = consumer.poll( 100 );
                if ( records.count() == 0 )
                {
                    this.hasFinished.set( true );
                }
                for ( ConsumerRecord<String, Operation> record : records )
                {
                    Operation operation = record.value();
                    operationExecutor.execute( operation );
                }
            }
        }
        catch ( Exception e )
        {
            errorReporter.reportError( this, ConcurrentErrorReporter.stackTraceToString( e ) );
        }
        finally
        {
            this.hasFinished.set( true );
        }
    }
}
package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.Operation;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class UpdatesProducer {

    private final KafkaProducer<String, Operation> kafkaProducer;
    private static final String TOPIC = "ldbc_updates";

    public UpdatesProducer( KafkaProducer<String, Operation> kafkaProducer ) {
        this.kafkaProducer = kafkaProducer;
    }

    public void send( Operation operation ) {
        this.kafkaProducer.send( new ProducerRecord<String, Operation>( TOPIC, operation ) );
        this.kafkaProducer.flush();
    }

    public void close() {
        if(this.kafkaProducer != null ) {
            this.kafkaProducer.close();
        }
    }
}

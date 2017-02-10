package com.ldbc.driver;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.util.Map;

public class OperationSerializer implements Closeable, AutoCloseable, Serializer<Operation>, Deserializer<Operation> {
    public OperationSerializer() {
    }

    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.addDefaultSerializer( Operation.class, new UpdateKyroSerializer() );
            return kryo;
        }
    };

    @Override
    public void configure( Map<String, ?> map, boolean b ) {
    }

    @Override
    public byte[] serialize( String s, Operation operation ) {
        ByteBufferOutput output = new ByteBufferOutput( 100 );
        kryos.get().writeObject( output, operation );
        return output.toBytes();
    }

    @Override
    public Operation deserialize( String s, byte[] bytes ) {
        try {
            return kryos.get().readObject( new ByteBufferInput( bytes ), Operation.class );
        } catch (Exception e) {
            throw new IllegalArgumentException( "Error reading bytes", e );
        }
    }

    public void close() {
    }

    private class UpdateKyroSerializer extends com.esotericsoftware.kryo.Serializer<Operation> {

        @Override
        public void write( Kryo kryo, Output output, Operation operation ) {
            operation.writeKyro( kryo, output );
        }

        @Override
        public Operation read( Kryo kryo, Input input, Class<Operation> aClass ) {
            Operation operation = null;
            int type = input.read();
            switch (type) {
                case 1001:
                    operation = LdbcUpdate1AddPerson.readKyro( input );
                    break;
                case 1002:
                    operation = LdbcUpdate2AddPostLike.readKyro( input );
                    break;
                case 1003:
                    operation = LdbcUpdate3AddCommentLike.readKyro( input );
                    break;
                case 1004:
                    operation = LdbcUpdate4AddForum.readKyro( input );
                    break;
                case 1005:
                    operation = LdbcUpdate5AddForumMembership.readKyro( input );
                    break;
                case 1006:
                    operation = LdbcUpdate6AddPost.readKyro( input );
                    break;
                case 1007:
                    operation = LdbcUpdate7AddComment.readKyro( input );
                    break;
                default:
                    throw new IllegalArgumentException( "unexpected type" );
            }
            return operation;
        }
    }
}

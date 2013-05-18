package com.ldbc.workloads;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ldbc.data.ByteIterator;
import com.ldbc.generator.ConstantGenerator;
import com.ldbc.generator.Generator;
import com.ldbc.generator.GeneratorBuilder;
import com.ldbc.generator.GeneratorBuilderFactory;
import com.ldbc.generator.DynamicRangeUniformNumberGenerator;
import com.ldbc.generator.ycsb.YcsbZipfianNumberGenerator;
import com.ldbc.workloads.ycsb.Distribution;
import com.ldbc.workloads.ycsb.WorkloadUtils;

public class TempTest
{
    @Test
    public void test()
    {
        Object o = (Object) new Integer( 1 );
        System.out.println( o.getClass().getName() );
        try
        {
            Constructor<String> constructor = String.class.getConstructor( String.class );
            String object = constructor.newInstance( new Object[] { "hello" } );
            System.out.println( object );
        }
        catch ( SecurityException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( NoSuchMethodException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( IllegalArgumentException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( InstantiationException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( IllegalAccessException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( InvocationTargetException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

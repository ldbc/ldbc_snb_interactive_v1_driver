
package OLD_com.ldbc.driver.workloads.ycsb;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.random.RandomDataGenerator;

import OLD_com.ldbc.driver.workloads.WorkloadException;

import com.ldbc.driver.data.ByteIterator;
import com.ldbc.driver.data.RandomByteIterator;
import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.util.Pair;

public class WorkloadUtils
{
    // TODO temp, this should be given in the constructor, remove later
    static RandomDataGenerator random = new RandomDataGenerator();

    public static Map<String, ByteIterator> buildValuedFields( Generator<Set<String>> fieldSelectionGenerator,
            Generator<Integer> valueLengthGenerator ) throws WorkloadException
    {
        Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        for ( String fieldName : fieldSelectionGenerator.next() )
        {
            ByteIterator data = new RandomByteIterator( valueLengthGenerator.next(), random );
            values.put( fieldName, data );
        }
        return values;
    }

    public static <T extends Number> Generator<T> buildFieldLengthGenerator( GeneratorBuilder generatorBuilder,
            Distribution distribution, T lowerBound, T upperBound ) throws WorkloadException
    {
        switch ( distribution )
        {
        case CONSTANT:
            if ( false == lowerBound.equals( upperBound ) )
            {
                throw new WorkloadException( "For ConstantGenerator lowerbound must equal upperbound" );
            }
            return generatorBuilder.constantGenerator( lowerBound ).build();
        case UNIFORM:
            return generatorBuilder.uniformNumberGenerator( lowerBound, upperBound ).build();
        case ZIPFIAN:
            return generatorBuilder.zipfianNumberGenerator( lowerBound, upperBound ).build();
        default:
            String errMsg = String.format( "Invalid Distribution [%s], use one of the following: %s, %s, %s, %s",
                    distribution, Distribution.CONSTANT, Distribution.UNIFORM, Distribution.ZIPFIAN );
            throw new WorkloadException( errMsg );
        }
    }

    public static Generator<Set<String>> buildFieldSelectionGenerator( GeneratorBuilder generatorBuilder,
            String fieldNamePrefix, int fieldCount, int fieldCountToRetrieve ) throws WorkloadException
    {
        if ( 1 > fieldCountToRetrieve || fieldCountToRetrieve > fieldCount )
        {
            throw new WorkloadException( "fieldCountToRetrieve must be in the range [1,fieldCount]" );
        }
        else if ( fieldCount == fieldCountToRetrieve )
        {
            Set<String> fields = new HashSet<String>();
            for ( int i = 0; i < fieldCount; i++ )
            {
                String field = fieldNamePrefix + i;
                fields.add( field );
            }
            return generatorBuilder.constantGenerator( fields ).build();
        }
        else
        {
            Set<Pair<Double, String>> fields = new HashSet<Pair<Double, String>>();
            for ( int i = 0; i < fieldCount; i++ )
            {
                Pair<Double, String> field = Pair.create( 1d, fieldNamePrefix + i );
                fields.add( field );
            }
            Generator<Integer> fieldsToRetrieveGenerator = generatorBuilder.constantGenerator( fieldCountToRetrieve ).build();
            return generatorBuilder.discreteMultiGenerator( fields, fieldsToRetrieveGenerator ).build();
        }
    }
}

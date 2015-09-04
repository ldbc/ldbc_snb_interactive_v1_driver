package com.ldbc.driver.workloads;

import com.ldbc.driver.util.MapUtils;

import java.util.HashMap;
import java.util.Map;

abstract public class OperationMixBuilder
{
    public static InterleaveOperationMixBuilder fromInterleaves()
    {
        return new InterleaveOperationMixBuilder();
    }

    public static FrequencyOperationMixBuilder fromFrequencies( long baseIntervalAsMilli )
    {
        return new FrequencyOperationMixBuilder( baseIntervalAsMilli );
    }

    public static class InterleaveOperationMixBuilder
    {
        private final Map<Integer,Long> interleaves = new HashMap<>();

        private InterleaveOperationMixBuilder()
        {

        }

        public InterleaveOperationMixBuilder addOperationInterleave( int operationType, long interleave )
        {
            interleaves.put( operationType, interleave );
            return this;
        }

        public OperationMix build()
        {
            return new OperationMix( interleaves, 1 );
        }
    }

    public static class FrequencyOperationMixBuilder
    {
        private final long baseIntervalAsMilli;
        private final Map<Integer,Long> interleaves = new HashMap<>();

        private FrequencyOperationMixBuilder( long baseIntervalAsMilli )
        {
            this.baseIntervalAsMilli = baseIntervalAsMilli;
        }

        public FrequencyOperationMixBuilder addOperationFrequency( int operationType, long frequency )
        {
            interleaves.put( operationType, frequency * baseIntervalAsMilli );
            return this;
        }

        public OperationMix build()
        {
            return new OperationMix( interleaves, baseIntervalAsMilli );
        }
    }

    public static class OperationMix
    {
        private final Map<Integer,Long> interleaves;
        private final long baseIntervalAsMilli;

        private OperationMix( Map<Integer,Long> interleaves, long baseIntervalAsMilli )
        {
            this.baseIntervalAsMilli = baseIntervalAsMilli;
            this.interleaves = interleaves;
        }

        public Map<Integer,Long> interleaves()
        {
            Map<Integer,Long> tempInterleaves = new HashMap<>();
            for ( Integer operationType : interleaves.keySet() )
            {
                tempInterleaves.put( operationType, interleaves.get( operationType ) );
            }
            return tempInterleaves;
        }

        public long interleaveFor( int operationType )
        {
            return interleaves.get( operationType );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }

            OperationMix that = (OperationMix) o;

            return !(interleaves != null ? !operationMixesAreEqual( that ) : that.interleaves != null);
        }

        private boolean operationMixesAreEqual( OperationMix that )
        {
            if ( this.interleaves.size() != that.interleaves.size() )
            {
                return false;
            }
            for ( int operationType : this.interleaves.keySet() )
            {
                if ( this.interleaveFor( operationType ) != that.interleaveFor( operationType ) )
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            int result = interleaves != null ? interleaves.hashCode() : 0;
            result = 31 * result + (int) (baseIntervalAsMilli ^ (baseIntervalAsMilli >>> 32));
            return result;
        }

        @Override
        public String toString()
        {
            return "OperationMix{\n" +
                   "interleaves=\n" + MapUtils.prettyPrint( interleaves, "\t" ) +
                   ", baseIntervalAsMilli=" + baseIntervalAsMilli +
                   "\n}";
        }
    }
}

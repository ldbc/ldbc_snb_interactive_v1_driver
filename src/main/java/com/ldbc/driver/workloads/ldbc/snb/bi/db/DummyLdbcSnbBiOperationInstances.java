package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9;

public class DummyLdbcSnbBiOperationInstances
{

    /*
    LONG READS
     */

    public static LdbcSnbBiQuery1 read1()
    {
        return new LdbcSnbBiQuery1( 1, 2 );
    }

    public static LdbcSnbBiQuery2 read2()
    {
        return new LdbcSnbBiQuery2( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery3 read3()
    {
        return new LdbcSnbBiQuery3( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery4 read4()
    {
        return new LdbcSnbBiQuery4( "tagClass", "1", 2 );
    }

    public static LdbcSnbBiQuery5 read5()
    {
        return new LdbcSnbBiQuery5( "ᚠ", 1 );
    }

    public static LdbcSnbBiQuery6 read6()
    {
        return new LdbcSnbBiQuery6( "さ", 1 );
    }

    public static LdbcSnbBiQuery7 read7()
    {
        return new LdbcSnbBiQuery7( "丵", 1 );
    }

    public static LdbcSnbBiQuery8 read8()
    {
        return new LdbcSnbBiQuery8( "ش", 1 );
    }

    public static LdbcSnbBiQuery9 read9()
    {
        return new LdbcSnbBiQuery9( "1", "ش", 1 );
    }

    public static LdbcSnbBiQuery10 read10()
    {
        return new LdbcSnbBiQuery10( "さ", 1 );
    }

    public static LdbcSnbBiQuery11 read11()
    {
        return new LdbcSnbBiQuery11( "1", "2", 3 );
    }

    public static LdbcSnbBiQuery12 read12()
    {
        return new LdbcSnbBiQuery12( 1, 2 );
    }

    public static LdbcSnbBiQuery13 read13()
    {
        return new LdbcSnbBiQuery13( "丵", 1 );
    }

    public static LdbcSnbBiQuery14 read14()
    {
        return new LdbcSnbBiQuery14( 1, 2 );
    }

    public static LdbcSnbBiQuery15 read15()
    {
        return new LdbcSnbBiQuery15( "1", 2 );
    }

    public static LdbcSnbBiQuery16 read16()
    {
        return new LdbcSnbBiQuery16( "1", "2", 3 );
    }

    public static LdbcSnbBiQuery17 read17()
    {
        return new LdbcSnbBiQuery17( "1", 2 );
    }

    public static LdbcSnbBiQuery18 read18()
    {
        return new LdbcSnbBiQuery18( 1, 2 );
    }

    public static LdbcSnbBiQuery19 read19()
    {
        return new LdbcSnbBiQuery19( "丵", "ᚠ", 1 );
    }

    public static LdbcSnbBiQuery20 read20()
    {
        return new LdbcSnbBiQuery20( 1 );
    }

    public static LdbcSnbBiQuery21 read21()
    {
        return new LdbcSnbBiQuery21( "1", 2 );
    }

    public static LdbcSnbBiQuery22 read22()
    {
        return new LdbcSnbBiQuery22( "丵", "ش", 1 );
    }

    public static LdbcSnbBiQuery23 read23()
    {
        return new LdbcSnbBiQuery23( "丵", 1 );
    }

    public static LdbcSnbBiQuery24 read24()
    {
        return new LdbcSnbBiQuery24( "1", 2 );
    }
}

package com.ldbc.driver.workloads.ldbc.snb.interactive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class InteractiveReadEventStreamReadersTestData
{
    public static final SimpleDateFormat DATE_FORMAT;

    static
    {
        DATE_FORMAT = new SimpleDateFormat( "yyyy-MM-dd" );
        DATE_FORMAT.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
    }

    public static final String QUERY_1_CSV_ROWS()
    {
        return ""
               + "10995117334833|John\n"
               + "14293651244033|Yang\n"
               + "6597070008725|A.\n"
               + "2199023331001|Chen";
    }

    public static final String QUERY_2_CSV_ROWS() throws ParseException
    {
        return ""
               + "12094628092905|" + DATE_FORMAT.parse( "2013-01-28" ).getTime() + "\n"
               + "9895606011404|" + DATE_FORMAT.parse( "2013-01-28" ).getTime() + "\n"
               + "14293651244033|" + DATE_FORMAT.parse( "2013-02-2" ).getTime() + "\n"
               + "13194139602632|" + DATE_FORMAT.parse( "2013-10-16" ).getTime();
    }

    public static final String QUERY_3_CSV_ROWS() throws ParseException
    {
        return ""
               + "9895605643992|" + DATE_FORMAT.parse( "2011-12-1" ).getTime() + "|53|Taiwan|Bulgaria\n"
               + "979201|" + DATE_FORMAT.parse( "2012-4-1" ).getTime() + "|64|Nicaragua|Afghanistan\n"
               + "129891|" + DATE_FORMAT.parse( "2011-05-1" ).getTime() + "|58|Colombia|Lithuania\n"
               + "13194140498760|" + DATE_FORMAT.parse( "2010-12-1" ).getTime() + "|53|Lithuania|Afghanistan";
    }

    public static final String QUERY_4_CSV_ROWS() throws ParseException
    {
        return ""
               + "12094628092905|" + DATE_FORMAT.parse( "2011-4-1" ).getTime() + "|43\n"
               + "9895606011404|" + DATE_FORMAT.parse( "2012-1-1" ).getTime() + "|36\n"
               + "14293651244033|" + DATE_FORMAT.parse( "2011-7-1" ).getTime() + "|57\n"
               + "13194139602632|" + DATE_FORMAT.parse( "2011-7-1" ).getTime() + "|81\n";
    }

    public static final String QUERY_5_CSV_ROWS() throws ParseException
    {
        return ""
               + "9895605643992|" + DATE_FORMAT.parse( "2012-12-15" ).getTime() + "\n"
               + "979201|" + DATE_FORMAT.parse( "2012-12-16" ).getTime() + "\n"
               + "129891|" + DATE_FORMAT.parse( "2012-12-14" ).getTime() + "\n"
               + "13194140498760|" + DATE_FORMAT.parse( "2012-12-12" ).getTime();
    }

    public static final String QUERY_6_CSV_ROWS()
    {
        return ""
               + "9895605643992|Jiang_Zemin\n"
               + "979201|Nino_Rota\n"
               + "129891|John_VI_of_Portugal\n"
               + "13194140498760|Nikolai_Gogol";
    }

    public static final String QUERY_7_CSV_ROWS()
    {
        return ""
               + "16492675436774\n"
               + "14293651330072\n"
               + "4398047140913\n"
               + "13194140823804";
    }

    public static final String QUERY_8_CSV_ROWS()
    {
        return ""
               + "15393164184077\n"
               + "15393163594341\n"
               + "7696582593995\n"
               + "15393162809578";
    }

    public static final String QUERY_9_CSV_ROWS() throws ParseException
    {
        return ""
               + "9895605643992|" + DATE_FORMAT.parse( "2011-12-22" ).getTime() + "\n"
               + "979201|" + DATE_FORMAT.parse( "2011-11-19" ).getTime() + "\n"
               + "129891|" + DATE_FORMAT.parse( "2011-11-20" ).getTime() + "\n"
               + "13194140498760|" + DATE_FORMAT.parse( "2011-12-1" ).getTime();
    }

    public static final String QUERY_10_CSV_ROWS()
    {
        return ""
               + "9895605643992|2\n"
               + "979201|4\n"
               + "129891|2\n"
               + "13194140498760|3";
    }

    public static final String QUERY_11_CSV_ROWS()
    {
        return ""
               + "9895605643992|Taiwan|2013\n"
               + "979201|Nicaragua|1998\n"
               + "129891|Colombia|1974\n"
               + "13194140498760|Lithuania|1984";
    }

    public static final String QUERY_12_CSV_ROWS()
    {
        return ""
               + "12094628092905|SoccerManager\n"
               + "9895606011404|Chancellor\n"
               + "14293651244033|EurovisionSongContestEntry\n"
               + "13194139602632|GolfPlayer";
    }

    public static final String QUERY_13_CSV_ROWS()
    {
        return ""
               + "9895605643992|1099512323797\n"
               + "979201|95384\n"
               + "129891|9895606000517\n"
               + "13194140498760|7696582276748";
    }

    public static final String QUERY_14_CSV_ROWS()
    {
        return ""
               + "9895605643992|4398046737628\n"
               + "979201|1277748\n"
               + "129891|6597069967720\n"
               + "13194140498760|3298534975254";
    }
}

package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class BiReadEventStreamReadersTestData
{
    public static final SimpleDateFormat DATE_FORMAT;

    static
    {
        DATE_FORMAT = new SimpleDateFormat( "yyyy-MM-dd" );
        DATE_FORMAT.setTimeZone( TimeZone.getTimeZone( "GMT" ) );
    }

    public static final String QUERY_1_CSV_ROWS()
    {
        return "date\n"
               + "1441351591755\n"
               + "1441351591756";
    }

    public static final String QUERY_2_CSV_ROWS() throws ParseException
    {
        return "date1|date2|countries|endDate|messageThreshold\n"
               + "1441351591755|1441351591755|countryA;countryB|1|10\n"
               + "1441351591755|1441351591755|countryA;countryC|2|20\n"
               + "1441351591755|1441351591756|countryB;countryD;countryE|" + Long.toString( Long.MAX_VALUE ) + "|30";
    }

    public static final String QUERY_3_CSV_ROWS() throws ParseException
    {
        return "range1Start|range1End|range2Start|range2End\n"
               + "1|2|3|4\n"
               + "5|6|7|8\n"
               + Long.MAX_VALUE + "|" + Long.MAX_VALUE + "|" + Long.MAX_VALUE + "|" + Long.MAX_VALUE;
    }

    public static final String QUERY_4_CSV_ROWS() throws ParseException
    {
        return "tagClass|country\n"
               + "Writer|Cameroon\n"
               + "Writer|Colombia\n"
               + "Writer|Niger\n"
               + "Writer|Sweden\n";
    }

    public static final String QUERY_5_CSV_ROWS() throws ParseException
    {
        return "country\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_6_CSV_ROWS()
    {
        return "tag\n"
               + "Justin_Timberlake\n"
               + "Josip_Broz_Tito\n"
               + "Barry_Manilow\n"
               + "Charles_Darwin";
    }

    public static final String QUERY_7_CSV_ROWS()
    {
        return "tag\n"
               + "Franz_Schubert\n"
               + "Bill_Clinton\n"
               + "Dante_Alighieri\n"
               + "Khalid_Sheikh_Mohammed";
    }

    public static final String QUERY_8_CSV_ROWS()
    {
        return "tag\n"
               + "Alanis_Morissette\n"
               + "\u00c9amon_de_Valera\n"
               + "Juhi_Chawla\n"
               + "Manuel_Noriega";
    }

    public static final String QUERY_9_CSV_ROWS() throws ParseException
    {
        return "tagClass1|tagClass2|threshold\n"
               + "Person|OfficeHolder|1\n"
               + "Person|Writer|2\n"
               + "Person|Single|3\n"
               + "Person|Country|4\n";
    }

    public static final String QUERY_10_CSV_ROWS()
    {
        return "tag\n"
               + "Franz_Schubert\n"
               + "Bill_Clinton\n"
               + "Dante_Alighieri\n"
               + "Khalid_Sheikh_Mohammed";
    }

    public static final String QUERY_11_CSV_ROWS()
    {
        return "country|blacklist\n"
               + "Cameroon|Writer;Reader\n"
               + "Colombia|Writer;Cake\n"
               + "Niger|Pie\n"
               + "Sweden|Writer\n";
    }

    public static final String QUERY_12_CSV_ROWS()
    {
        return "creationDate|likeCount\n"
               + "1441351591755|1\n"
               + "1441351591756|2";
    }

    public static final String QUERY_13_CSV_ROWS()
    {
        return "country\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_14_CSV_ROWS()
    {
        return "begin|todoEnd\n"
               + "1441351591755|1441351591756\n"
               + "1441351591756|1441351591757";
    }

    public static final String QUERY_15_CSV_ROWS()
    {
        return "country\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_16_CSV_ROWS()
    {
        return "todoPerson|tag|country\n"
               + "1|Writer|Cameroon\n"
               + "2|Writer|Colombia\n"
               + "3|Writer|Niger\n"
               + "4|Writer|Sweden";
    }

    public static final String QUERY_17_CSV_ROWS()
    {
        return "country\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_18_CSV_ROWS()
    {
        return "creationDate\n"
               + "1441351591755\n"
               + "1441351591756";
    }

    public static final String QUERY_19_CSV_ROWS()
    {
        return "date|tagClass1|tagClass2\n"
               + "1|Writer|Single\n"
               + "2|Writer|Country\n"
               + "3|Writer|Album\n"
               + "4|Writer|BritishRoyalty";
    }

    public static final String QUERY_20_CSV_ROWS()
    {
        return "tagClasses\n"
               + "a;b;c\n"
               + "b;c\n"
               + "c";
    }

    public static final String QUERY_21_CSV_ROWS()
    {
        return "country|endDate\n"
               + "Kenya|1\n"
               + "Peru|2\n"
               + "Tunisia|3\n"
               + "Venezuela|4";
    }

    public static final String QUERY_22_CSV_ROWS()
    {
        return "country1|country2\n"
               + "Germany|Pakistan\n"
               + "Germany|Russia\n"
               + "Germany|Vietnam\n"
               + "Germany|Philippines\n";
    }

    public static final String QUERY_23_CSV_ROWS()
    {
        return "country\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_24_CSV_ROWS()
    {
        return "tagClass\n"
               + "Person\n"
               + "OfficeHolder\n"
               + "Writer\n"
               + "Single\n";
    }
}

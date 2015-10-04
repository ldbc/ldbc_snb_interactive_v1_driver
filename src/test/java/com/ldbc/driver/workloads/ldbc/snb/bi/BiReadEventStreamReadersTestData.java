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
        return "Param0\n"
               + "1441351591755\n"
               + "1441351591756";
    }

    public static final String QUERY_2_CSV_ROWS() throws ParseException
    {
        return "Param0|Param1|Param2|Param3\n"
               + "1441351591755|1441351591755|countryA|countryB\n"
               + "1441351591755|1441351591755|countryA|countryC\n"
               + "1441351591755|1441351591756|countryB|countryD";
    }

    public static final String QUERY_3_CSV_ROWS() throws ParseException
    {
        return "Year|Month\n"
               + "2013|1\n"
               + "1982|4\n"
               + "2008|12";
    }

    public static final String QUERY_4_CSV_ROWS() throws ParseException
    {
        return "Param0|Param1\n"
               + "Writer|Cameroon\n"
               + "Writer|Colombia\n"
               + "Writer|Niger\n"
               + "Writer|Sweden\n";
    }

    public static final String QUERY_5_CSV_ROWS() throws ParseException
    {
        return "Param0\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_6_CSV_ROWS()
    {
        return "Param0\n"
               + "Justin_Timberlake\n"
               + "Josip_Broz_Tito\n"
               + "Barry_Manilow\n"
               + "Charles_Darwin";
    }

    public static final String QUERY_7_CSV_ROWS()
    {
        return "Param0\n"
               + "Franz_Schubert\n"
               + "Bill_Clinton\n"
               + "Dante_Alighieri\n"
               + "Khalid_Sheikh_Mohammed";
    }

    public static final String QUERY_8_CSV_ROWS()
    {
        return "Param0\n"
               + "Alanis_Morissette\n"
               + "\u00c9amon_de_Valera\n"
               + "Juhi_Chawla\n"
               + "Manuel_Noriega";
    }

    public static final String QUERY_9_CSV_ROWS() throws ParseException
    {
        return "Param0|Param1\n"
               + "Person|OfficeHolder\n"
               + "Person|Writer\n"
               + "Person|Single\n"
               + "Person|Country\n";
    }

    public static final String QUERY_10_CSV_ROWS()
    {
        return "Param0\n"
               + "Franz_Schubert\n"
               + "Bill_Clinton\n"
               + "Dante_Alighieri\n"
               + "Khalid_Sheikh_Mohammed";
    }

    public static final String QUERY_11_CSV_ROWS()
    {
        return "Param0|Param1\n"
               + "Writer|Cameroon\n"
               + "Writer|Colombia\n"
               + "Writer|Niger\n"
               + "Writer|Sweden\n";
    }

    public static final String QUERY_12_CSV_ROWS()
    {
        return "Param0\n"
               + "1441351591755\n"
               + "1441351591756";
    }

    public static final String QUERY_13_CSV_ROWS()
    {
        return "Param0\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_14_CSV_ROWS()
    {
        return "Param0\n"
               + "1441351591755\n"
               + "1441351591756";
    }

    public static final String QUERY_15_CSV_ROWS()
    {
        return "Param0\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_16_CSV_ROWS()
    {
        return "Param0|Param1\n"
               + "Writer|Cameroon\n"
               + "Writer|Colombia\n"
               + "Writer|Niger\n"
               + "Writer|Sweden";
    }

    public static final String QUERY_17_CSV_ROWS()
    {
        return "Param0\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_18_CSV_ROWS()
    {
        return "Param0\n"
               + "1441351591755\n"
               + "1441351591756";
    }

    public static final String QUERY_19_CSV_ROWS()
    {
        return "Param0|Param1\n"
               + "Writer|Single\n"
               + "Writer|Country\n"
               + "Writer|Album\n"
               + "Writer|BritishRoyalty";
    }

    public static final String QUERY_20_CSV_ROWS()
    {
        return "";
    }

    public static final String QUERY_21_CSV_ROWS()
    {
        return "Param0\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_22_CSV_ROWS()
    {
        return "Param0|Param1\n"
               + "Germany|Pakistan\n"
               + "Germany|Russia\n"
               + "Germany|Vietnam\n"
               + "Germany|Philippines\n";
    }

    public static final String QUERY_23_CSV_ROWS()
    {
        return "Param0\n"
               + "Kenya\n"
               + "Peru\n"
               + "Tunisia\n"
               + "Venezuela";
    }

    public static final String QUERY_24_CSV_ROWS()
    {
        return "Param0\n"
               + "Person\n"
               + "OfficeHolder\n"
               + "Writer\n"
               + "Single\n";
    }
}

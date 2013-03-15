package com.yahoo.ycsb.workloads;

public class CoreWorkloadProperties
{
    /**
     * length distribution: "uniform", "zipfian" (favoring short records),
     * "constant", "histogram"
     * 
     * "uniform", "zipfian", "constant": "fieldlength" defines max field length
     * 
     * "histogram": histogram read from "fieldlengthhistogram" filename
     */
    public static final String FIELD_LENGTH_DISTRIBUTION = "fieldlengthdistribution";
    public static final String FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

    /**
     * length of a field in bytes
     */
    public static final String FIELD_LENGTH = "fieldlength";
    public static final String FIELD_LENGTH_DEFAULT = "100";

    /**
     * filename containing the field length histogram (used if
     * "fieldlengthdistribution" is "histogram")
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE = "fieldlengthhistogram";
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_DEFAULT = "hist.txt";

    /**
     * database table to run queries against
     */
    public static final String TABLENAME = "table";
    public static final String TABLENAME_DEFAULT = "usertable";

    /**
     * number of fields in a record
     */
    public static final String FIELD_COUNT = "fieldcount";
    public static final String FIELD_COUNT_DEFAULT = "10";

    /**
     * read one field (false) or all fields (true) of a record
     */
    public static final String READ_ALL_FIELDS = "readallfields";
    public static final String READ_ALL_FIELDS_DEFAULT = "true";

    /**
     * write one field (false) or all fields (true) of a record
     */
    public static final String WRITE_ALL_FIELDS = "writeallfields";
    public static final String WRITE_ALL_FIELDS_DEFAULT = "false";

    /**
     * proportion of transactions that are reads
     */
    public static final String READ_PROPORTION = "readproportion";
    public static final String READ_PROPORTION_DEFAULT = "0.95";

    /**
     * proportion of transactions that are updates
     */
    public static final String UPDATE_PROPORTION = "updateproportion";
    public static final String UPDATE_PROPORTION_DEFAULT = "0.05";

    /**
     * proportion of transactions that are inserts
     */
    public static final String INSERT_PROPORTION = "insertproportion";
    public static final String INSERT_PROPORTION_DEFAULT = "0.0";

    /**
     * proportion of transactions that are scans
     */
    public static final String SCAN_PROPORTION = "scanproportion";
    public static final String SCAN_PROPORTION_DEFAULT = "0.0";

    /**
     * proportion of transactions that are read-modify-write
     */
    public static final String READMODIFYWRITE_PROPORTION = "readmodifywriteproportion";
    public static final String READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

    /**
     * distribution of requests across keyspace: "uniform", "zipfian", "latest"
     */
    public static final String REQUEST_DISTRIBUTION = "requestdistribution";
    public static final String REQUEST_DISTRIBUTION_DEFAULT = "uniform";

    /**
     * max scan length (number of records)
     */
    public static final String MAX_SCAN_LENGTH = "maxscanlength";
    public static final String MAX_SCAN_LENGTH_DEFAULT = "1000";

    /**
     * scan length distribution: "uniform", "zipfian" (favoring short scans)
     */
    public static final String SCAN_LENGTH_DISTRIBUTION = "scanlengthdistribution";
    public static final String SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

    /**
     * order to insert records: "ordered", "hashed"
     */
    public static final String INSERT_ORDER = "insertorder";
    public static final String INSERT_ORDER_DEFAULT = "hashed";

    /**
     * percentage data items that constitute the hot set
     */
    public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
    public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

    /**
     * percentage operations that access the hot set
     */
    public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
    public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

}

package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class SubstitutionParameters
{
    /*
    DETAILS
     - http://www.ldbc.eu:8090/display/TUC/IW+Substitution+parameters+selection
    
    GENERATE
     - personNames.txt
     - personNumber.txt
     - creationPostDate.txt 
         0,33,66,100 percents of date range... or maybe all 0..100
         duration of date range
     - tagUris.txt
     - countryUris.txt (orgLocations.txt)
     - workFromDate.txt
     - tagClassUris.txt
      
    PROVIDED 
     - countryPairs.txt
     */

    @JsonProperty( value = COUNTRY_PAIRS )
    public List<String[]> countryPairs = null;
    @JsonProperty( value = FIRST_NAMES )
    public List<String> firstNames = null;
    @JsonProperty( value = POST_CREATION_DATES )
    public Map<Integer, Long> postCreationDates = null;
    @JsonProperty( value = PERSON_IDS )
    public List<Long> personIds = null;
    @JsonProperty( value = TAG_URIS )
    public List<String> tagUris = null;
    @JsonProperty( value = HOROSCOPE_SIGNS )
    public List<Integer> horoscopeSigns = null;
    @JsonProperty( value = COUNTRY_URIS )
    public List<String> countryUris = null;
    @JsonProperty( value = WORK_FROM_DATES )
    public List<Integer> workFromDates = null;
    @JsonProperty( value = TAG_CLASS_URIS )
    public List<String> tagClassUris = null;

    public SubstitutionParameters()
    {
    }

    private static final String COUNTRY_PAIRS = "countryPairs";
    private static final String FIRST_NAMES = "first_names";
    private static final String POST_CREATION_DATES = "post_creation_dates";
    private static final String PERSON_IDS = "person_ids";
    private static final String TAG_URIS = "tag_uris";
    private static final String HOROSCOPE_SIGNS = "horoscope_signs";
    private static final String COUNTRY_URIS = "country_uris";
    private static final String WORK_FROM_DATES = "work_from_dates";
    private static final String TAG_CLASS_URIS = "tag_class_uris";

    public static SubstitutionParameters fromJson( File jsonFile ) throws JsonParseException, JsonMappingException,
            IOException
    {
        return new ObjectMapper().readValue( jsonFile, SubstitutionParameters.class );
    }

    @Override
    public String toString()
    {
        try
        {
            return new ObjectMapper().writeValueAsString( this );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Unable to generate String", e.getCause() );
        }
    }
}

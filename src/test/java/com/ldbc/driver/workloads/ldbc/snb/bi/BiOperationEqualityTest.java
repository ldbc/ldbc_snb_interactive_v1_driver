package com.ldbc.driver.workloads.ldbc.snb.bi;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class BiOperationEqualityTest
{
    @Test
    public void ldbcQuery1ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;
        int limit1 = 1;

        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery1 query1a = new LdbcSnbBiQuery1( date1, limit1 );
        LdbcSnbBiQuery1 query1b = new LdbcSnbBiQuery1( date1, limit1 );
        LdbcSnbBiQuery1 query2a = new LdbcSnbBiQuery1( date2, limit2 );
        LdbcSnbBiQuery1 query3a = new LdbcSnbBiQuery1( date1, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery2ShouldDoEqualsCorrectly()
    {
        // Given
        long dateA1 = 1;
        long dateB1 = 1;
        int limit1 = 1;

        long dateA2 = 2;
        long dateB2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery2 query1a = new LdbcSnbBiQuery2( dateA1, dateB1, limit1 );
        LdbcSnbBiQuery2 query1b = new LdbcSnbBiQuery2( dateA1, dateB1, limit1 );
        LdbcSnbBiQuery2 query2a = new LdbcSnbBiQuery2( dateA2, dateB2, limit2 );
        LdbcSnbBiQuery2 query3a = new LdbcSnbBiQuery2( dateA1, dateB2, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery3ShouldDoEqualsCorrectly()
    {
        // Given
        long dateA1 = 1;
        long dateB1 = 1;
        int limit1 = 1;

        long dateA2 = 2;
        long dateB2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery3 query1a = new LdbcSnbBiQuery3( dateA1, dateB1, limit1 );
        LdbcSnbBiQuery3 query1b = new LdbcSnbBiQuery3( dateA1, dateB1, limit1 );
        LdbcSnbBiQuery3 query2a = new LdbcSnbBiQuery3( dateA2, dateB2, limit2 );
        LdbcSnbBiQuery3 query3a = new LdbcSnbBiQuery3( dateA1, dateB2, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery4ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClass1 = "1";
        String country1 = "さ";
        int limit1 = 1;

        String tagClass2 = "2";
        String country2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery4 query1a = new LdbcSnbBiQuery4( tagClass1, country1, limit1 );
        LdbcSnbBiQuery4 query1b = new LdbcSnbBiQuery4( tagClass1, country1, limit1 );
        LdbcSnbBiQuery4 query2a = new LdbcSnbBiQuery4( tagClass2, country2, limit2 );
        LdbcSnbBiQuery4 query3a = new LdbcSnbBiQuery4( tagClass2, country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery5ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "さ";
        int limit1 = 1;

        String country2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery5 query1a = new LdbcSnbBiQuery5( country1, limit1 );
        LdbcSnbBiQuery5 query1b = new LdbcSnbBiQuery5( country1, limit1 );
        LdbcSnbBiQuery5 query2a = new LdbcSnbBiQuery5( country2, limit2 );
        LdbcSnbBiQuery5 query3a = new LdbcSnbBiQuery5( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery6ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "さ";
        int limit1 = 1;

        String tag2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery6 query1a = new LdbcSnbBiQuery6( tag1, limit1 );
        LdbcSnbBiQuery6 query1b = new LdbcSnbBiQuery6( tag1, limit1 );
        LdbcSnbBiQuery6 query2a = new LdbcSnbBiQuery6( tag2, limit2 );
        LdbcSnbBiQuery6 query3a = new LdbcSnbBiQuery6( tag2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery7ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "さ";
        int limit1 = 1;

        String tag2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery7 query1a = new LdbcSnbBiQuery7( tag1, limit1 );
        LdbcSnbBiQuery7 query1b = new LdbcSnbBiQuery7( tag1, limit1 );
        LdbcSnbBiQuery7 query2a = new LdbcSnbBiQuery7( tag2, limit2 );
        LdbcSnbBiQuery7 query3a = new LdbcSnbBiQuery7( tag2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery8ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "さ";
        int limit1 = 1;

        String tag2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery8 query1a = new LdbcSnbBiQuery8( tag1, limit1 );
        LdbcSnbBiQuery8 query1b = new LdbcSnbBiQuery8( tag1, limit1 );
        LdbcSnbBiQuery8 query2a = new LdbcSnbBiQuery8( tag2, limit2 );
        LdbcSnbBiQuery8 query3a = new LdbcSnbBiQuery8( tag2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery9ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClassA1 = "さ";
        String tagClassB1 = "1";
        int limit1 = 1;

        String tagClassA2 = "丵";
        String tagClassB2 = "2";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery9 query1a = new LdbcSnbBiQuery9( tagClassA1, tagClassB1, limit1 );
        LdbcSnbBiQuery9 query1b = new LdbcSnbBiQuery9( tagClassA1, tagClassB1, limit1 );
        LdbcSnbBiQuery9 query2a = new LdbcSnbBiQuery9( tagClassA2, tagClassB2, limit2 );
        LdbcSnbBiQuery9 query3a = new LdbcSnbBiQuery9( tagClassA2, tagClassB2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery10ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "さ";
        int limit1 = 1;

        String tag2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery10 query1a = new LdbcSnbBiQuery10( tag1, limit1 );
        LdbcSnbBiQuery10 query1b = new LdbcSnbBiQuery10( tag1, limit1 );
        LdbcSnbBiQuery10 query2a = new LdbcSnbBiQuery10( tag2, limit2 );
        LdbcSnbBiQuery10 query3a = new LdbcSnbBiQuery10( tag2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery11ShouldDoEqualsCorrectly()
    {
        // Given
        String keyWord1 = "さ";
        String country1 = "ᚠ";
        int limit1 = 1;

        String keyWord2 = "丵";
        String country2 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery11 query1a = new LdbcSnbBiQuery11( keyWord1, country1, limit1 );
        LdbcSnbBiQuery11 query1b = new LdbcSnbBiQuery11( keyWord1, country1, limit1 );
        LdbcSnbBiQuery11 query2a = new LdbcSnbBiQuery11( keyWord2, country2, limit2 );
        LdbcSnbBiQuery11 query3a = new LdbcSnbBiQuery11( keyWord2, country1, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery12ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;
        int limit1 = 1;

        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery12 query1a = new LdbcSnbBiQuery12( date1, limit1 );
        LdbcSnbBiQuery12 query1b = new LdbcSnbBiQuery12( date1, limit1 );
        LdbcSnbBiQuery12 query2a = new LdbcSnbBiQuery12( date2, limit2 );
        LdbcSnbBiQuery12 query3a = new LdbcSnbBiQuery12( date1, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery13ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "さ";
        int limit1 = 1;

        String country2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery13 query1a = new LdbcSnbBiQuery13( country1, limit1 );
        LdbcSnbBiQuery13 query1b = new LdbcSnbBiQuery13( country1, limit1 );
        LdbcSnbBiQuery13 query2a = new LdbcSnbBiQuery13( country2, limit2 );
        LdbcSnbBiQuery13 query3a = new LdbcSnbBiQuery13( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery14ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;
        int limit1 = 1;

        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery14 query1a = new LdbcSnbBiQuery14( date1, limit1 );
        LdbcSnbBiQuery14 query1b = new LdbcSnbBiQuery14( date1, limit1 );
        LdbcSnbBiQuery14 query2a = new LdbcSnbBiQuery14( date2, limit2 );
        LdbcSnbBiQuery14 query3a = new LdbcSnbBiQuery14( date1, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery15ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "さ";
        int limit1 = 1;

        String country2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery15 query1a = new LdbcSnbBiQuery15( country1, limit1 );
        LdbcSnbBiQuery15 query1b = new LdbcSnbBiQuery15( country1, limit1 );
        LdbcSnbBiQuery15 query2a = new LdbcSnbBiQuery15( country2, limit2 );
        LdbcSnbBiQuery15 query3a = new LdbcSnbBiQuery15( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery16ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClass1 = "さ";
        String country1 = "ᚠ";
        int limit1 = 1;

        String tagClass2 = "丵";
        String country2 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery16 query1a = new LdbcSnbBiQuery16( tagClass1, country1, limit1 );
        LdbcSnbBiQuery16 query1b = new LdbcSnbBiQuery16( tagClass1, country1, limit1 );
        LdbcSnbBiQuery16 query2a = new LdbcSnbBiQuery16( tagClass2, country2, limit2 );
        LdbcSnbBiQuery16 query3a = new LdbcSnbBiQuery16( tagClass2, country1, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery17ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "さ";
        int limit1 = 1;

        String country2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery17 query1a = new LdbcSnbBiQuery17( country1, limit1 );
        LdbcSnbBiQuery17 query1b = new LdbcSnbBiQuery17( country1, limit1 );
        LdbcSnbBiQuery17 query2a = new LdbcSnbBiQuery17( country2, limit2 );
        LdbcSnbBiQuery17 query3a = new LdbcSnbBiQuery17( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery18ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;
        int limit1 = 1;

        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery18 query1a = new LdbcSnbBiQuery18( date1, limit1 );
        LdbcSnbBiQuery18 query1b = new LdbcSnbBiQuery18( date1, limit1 );
        LdbcSnbBiQuery18 query2a = new LdbcSnbBiQuery18( date2, limit2 );
        LdbcSnbBiQuery18 query3a = new LdbcSnbBiQuery18( date1, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery19ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClassA1 = "さ";
        String tagClassB1 = "ᚠ";
        int limit1 = 1;

        String tagClassA2 = "丵";
        String tagClassB2 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery16 query1a = new LdbcSnbBiQuery16( tagClassA1, tagClassB1, limit1 );
        LdbcSnbBiQuery16 query1b = new LdbcSnbBiQuery16( tagClassA1, tagClassB1, limit1 );
        LdbcSnbBiQuery16 query2a = new LdbcSnbBiQuery16( tagClassA2, tagClassB2, limit2 );
        LdbcSnbBiQuery16 query3a = new LdbcSnbBiQuery16( tagClassA2, tagClassB1, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery20ShouldDoEqualsCorrectly()
    {
        // Given
        int limit1 = 1;

        int limit2 = 2;

        // When
        LdbcSnbBiQuery20 query1a = new LdbcSnbBiQuery20( limit1 );
        LdbcSnbBiQuery20 query1b = new LdbcSnbBiQuery20( limit1 );
        LdbcSnbBiQuery20 query2a = new LdbcSnbBiQuery20( limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
    }

    @Test
    public void ldbcQuery21ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "さ";
        int limit1 = 1;

        String country2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery21 query1a = new LdbcSnbBiQuery21( country1, limit1 );
        LdbcSnbBiQuery21 query1b = new LdbcSnbBiQuery21( country1, limit1 );
        LdbcSnbBiQuery21 query2a = new LdbcSnbBiQuery21( country2, limit2 );
        LdbcSnbBiQuery21 query3a = new LdbcSnbBiQuery21( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery22ShouldDoEqualsCorrectly()
    {
        // Given
        String countryA1 = "さ";
        String countryB1 = "ᚠ";
        int limit1 = 1;

        String countryA2 = "丵";
        String countryB2 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery22 query1a = new LdbcSnbBiQuery22( countryA1, countryB1, limit1 );
        LdbcSnbBiQuery22 query1b = new LdbcSnbBiQuery22( countryA1, countryB1, limit1 );
        LdbcSnbBiQuery22 query2a = new LdbcSnbBiQuery22( countryA2, countryB2, limit2 );
        LdbcSnbBiQuery22 query3a = new LdbcSnbBiQuery22( countryA2, countryB1, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery23ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "さ";
        int limit1 = 1;

        String country2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery23 query1a = new LdbcSnbBiQuery23( country1, limit1 );
        LdbcSnbBiQuery23 query1b = new LdbcSnbBiQuery23( country1, limit1 );
        LdbcSnbBiQuery23 query2a = new LdbcSnbBiQuery23( country2, limit2 );
        LdbcSnbBiQuery23 query3a = new LdbcSnbBiQuery23( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery24ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClass1 = "さ";
        int limit1 = 1;

        String tagClass2 = "丵";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery24 query1a = new LdbcSnbBiQuery24( tagClass1, limit1 );
        LdbcSnbBiQuery24 query1b = new LdbcSnbBiQuery24( tagClass1, limit1 );
        LdbcSnbBiQuery24 query2a = new LdbcSnbBiQuery24( tagClass2, limit2 );
        LdbcSnbBiQuery24 query3a = new LdbcSnbBiQuery24( tagClass2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }
}
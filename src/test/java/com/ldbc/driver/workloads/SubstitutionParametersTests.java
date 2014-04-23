package com.ldbc.driver.workloads;

import com.ldbc.driver.workloads.ldbc.socnet.interactive.SubstitutionParameters;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SubstitutionParametersTests {
    static String SAMPLE_JSON = "{" +
            "\"minPersonId\":0," +
            "\"maxPersonId\":1999," +
            "\"minWorkFrom\":\"1970\"," +
            "\"maxWorkFrom\":\"2024\"," +
            "\"minPostCreationDate\":\"2010\"," +
            "\"maxPostCreationDate\":\"2011\"," +
            "\"firstNames\":[\"name1\",\"name2\",\"name3\"]," +
            "\"tagNames\":[\"tag1\",\"tag2\"]," +
            "\"countries\":[\"country1\",\"country2\"]," +
            "\"tagClasses\":[\"class1\"]," +
            "\"flashmobTags\":[{\"level\":1,\"date\":1,\"prob\":0.01,\"tag\":1},{\"level\":2,\"date\":2,\"prob\":0.02,\"tag\":2}]," +
            "\"countryPairs\":[[\"country1\",\"country2\"],[\"country3\",\"country4\"],[\"country5\",\"country6\"]]" +
            "}";

    @Test
    public void shouldLoadFromString() throws IOException {
        SubstitutionParameters parameters = SubstitutionParameters.fromJson(SAMPLE_JSON);
        assertThat(parameters.minPersonId, is(0L));
        assertThat(parameters.maxPersonId, is(1999L));
        assertThat(parameters.minWorkFrom, is(1970));
        assertThat(parameters.maxWorkFrom, is(2024));
        assertThat(parameters.minPostCreationDate, is(2010));
        assertThat(parameters.maxPostCreationDate, is(2011));
        assertThat(parameters.firstNames.size(), is(3));
        assertThat(parameters.firstNames.get(0), is("name1"));
        assertThat(parameters.firstNames.get(1), is("name2"));
        assertThat(parameters.firstNames.get(2), is("name3"));
        assertThat(parameters.tagNames.size(), is(2));
        assertThat(parameters.tagNames.get(0), is("tag1"));
        assertThat(parameters.tagNames.get(1), is("tag2"));
        assertThat(parameters.countries.size(), is(2));
        assertThat(parameters.countries.get(0), is("country1"));
        assertThat(parameters.countries.get(1), is("country2"));
        assertThat(parameters.tagClasses.size(), is(1));
        assertThat(parameters.tagClasses.get(0), is("class1"));
        assertThat(parameters.flashmobTags.size(), is(2));
        assertThat(parameters.flashmobTags.get(0).level, is(1));
        assertThat(parameters.flashmobTags.get(0).date, is(1L));
        assertThat(parameters.flashmobTags.get(0).prob, is(0.01));
        assertThat(parameters.flashmobTags.get(0).tag, is(1));
        assertThat(parameters.flashmobTags.get(1).level, is(2));
        assertThat(parameters.flashmobTags.get(1).date, is(2L));
        assertThat(parameters.flashmobTags.get(1).prob, is(0.02));
        assertThat(parameters.flashmobTags.get(1).tag, is(2));
        assertThat(parameters.countryPairs.size(), is(3));
        assertThat(parameters.countryPairs.get(0).length, is(2));
        assertThat(parameters.countryPairs.get(0)[0], is("country1"));
        assertThat(parameters.countryPairs.get(0)[1], is("country2"));
        assertThat(parameters.countryPairs.get(1).length, is(2));
        assertThat(parameters.countryPairs.get(1)[0], is("country3"));
        assertThat(parameters.countryPairs.get(1)[1], is("country4"));
        assertThat(parameters.countryPairs.get(2).length, is(2));
        assertThat(parameters.countryPairs.get(2)[0], is("country5"));
        assertThat(parameters.countryPairs.get(2)[1], is("country6"));
    }
}

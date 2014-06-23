package com.ldbc.driver.workloads.ldbc.snb.interactive;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

@JsonIgnoreProperties({/*TODO REMOVE*/"eventParams", "countryIds", /*TODO IMPLEMENT*/ "deltaTime", "minUpdateStream", "maxUpdateStream"})
public class SubstitutionParameters {
    @JsonProperty(value = "minPersonId")
    public Long minPersonId = null;
    @JsonProperty(value = "maxPersonId")
    public Long maxPersonId = null;
    @JsonProperty(value = "minWorkFrom")
    public Integer minWorkFrom = null;
    @JsonProperty(value = "maxWorkFrom")
    public Integer maxWorkFrom = null;
    @JsonProperty(value = "minPostCreationDate")
    public Integer minPostCreationDate = null;
    @JsonProperty(value = "maxPostCreationDate")
    public Integer maxPostCreationDate = null;
    @JsonProperty(value = "firstNames")
    public List<String> firstNames = null;
    @JsonProperty(value = "tagNames")
    public List<String> tagNames = null;
    @JsonProperty(value = "countries")
    public List<String> countries = null;
    @JsonProperty(value = "tagClasses")
    public List<String> tagClasses = null;
    @JsonProperty(value = "flashmobTags")
    public List<FlashmobTags> flashmobTags = null;
    @JsonProperty(value = "countryPairs")
    public List<String[]> countryPairs = null;

    public SubstitutionParameters() {
    }

    public static SubstitutionParameters fromJson(File jsonFile) throws
            IOException {
        return new ObjectMapper().readValue(jsonFile, SubstitutionParameters.class);
    }

    public static SubstitutionParameters fromJson(String jsonString) throws IOException {
        return new ObjectMapper().readValue(jsonString, SubstitutionParameters.class);
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Unable to generate parameter values string", e);
        }
    }

    public static class FlashmobTags {
        @JsonProperty(value = "level")
        public Integer level = null;
        @JsonProperty(value = "date")
        public Long date = null;
        @JsonProperty(value = "prob")
        public Double prob = null;
        @JsonProperty(value = "tag")
        public Integer tag = null;

        public FlashmobTags() {
        }

        @Override
        public String toString() {
            try {
                return new ObjectMapper().writeValueAsString(this);
            } catch (Exception e) {
                throw new RuntimeException("Unable to generate Flashmob string", e);
            }
        }
    }
}

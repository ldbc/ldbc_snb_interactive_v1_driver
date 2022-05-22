package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcQuery6Result {
    private final String tagName;
    private final int postCount;

    public LdbcQuery6Result(
        @JsonProperty("tagName")   String tagName,
        @JsonProperty("postCount") int postCount
    )
    {
        this.tagName = tagName;
        this.postCount = postCount;
    }

    public String getTagName() {
        return tagName;
    }

    public int getPostCount() {
        return postCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery6Result that = (LdbcQuery6Result) o;

        if (postCount != that.postCount) return false;
        if (tagName != null ? !tagName.equals(that.tagName) : that.tagName != null) return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery6Result{" +
                "tagName='" + tagName + '\'' +
                ", postCount=" + postCount +
                '}';
    }
}

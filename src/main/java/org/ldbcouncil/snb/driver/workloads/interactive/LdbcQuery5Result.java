package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcQuery5Result {
    private final String forumTitle;
    private final int postCount;

    public LdbcQuery5Result(
        @JsonProperty("forumTitle")String forumTitle,
        @JsonProperty("postCount")int postCount
    ) {
        this.forumTitle = forumTitle;
        this.postCount = postCount;
    }

    public String getForumTitle() {
        return forumTitle;
    }

    public int getPostCount() {
        return postCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery5Result result = (LdbcQuery5Result) o;

        if (postCount != result.postCount) return false;
        if (forumTitle != null ? !forumTitle.equals(result.forumTitle) : result.forumTitle != null) return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery5Result{" +
                "forumTitle='" + forumTitle + '\'' +
                ", postCount=" + postCount +
                '}';
    }
}

package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery5Result {
    private final String forumTitle;
    private final long postCount;

    public LdbcQuery5Result(String forumTitle, long postCount) {
        super();
        this.forumTitle = forumTitle;
        this.postCount = postCount;
    }

    public String forumTitle() {
        return forumTitle;
    }

    public long postCount() {
        return postCount;
    }

    public long count() {
        return postCount;
    }

    @Override
    public String toString() {
        return "LdbcQuery5Result{" +
                "forumTitle='" + forumTitle + '\'' +
                ", postCount=" + postCount +
                '}';
    }
}

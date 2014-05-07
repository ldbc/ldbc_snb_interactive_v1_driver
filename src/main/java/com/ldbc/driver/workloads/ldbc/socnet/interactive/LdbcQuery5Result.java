package com.ldbc.driver.workloads.ldbc.socnet.interactive;

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery5Result that = (LdbcQuery5Result) o;

        if (postCount != that.postCount) return false;
        if (forumTitle != null ? !forumTitle.equals(that.forumTitle) : that.forumTitle != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = forumTitle != null ? forumTitle.hashCode() : 0;
        result = 31 * result + (int) (postCount ^ (postCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery5Result{" +
                "forumTitle='" + forumTitle + '\'' +
                ", postCount=" + postCount +
                '}';
    }
}

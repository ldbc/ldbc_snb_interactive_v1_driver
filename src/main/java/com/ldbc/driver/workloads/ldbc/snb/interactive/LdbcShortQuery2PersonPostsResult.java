package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery2PersonPostsResult {
    private final long postId;
    private final String postContent;

    public LdbcShortQuery2PersonPostsResult(long postId, String postContent) {
        this.postId = postId;
        this.postContent = postContent;
    }

    public long postId() {
        return postId;
    }

    public String postContent() {
        return postContent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery2PersonPostsResult that = (LdbcShortQuery2PersonPostsResult) o;

        if (postId != that.postId) return false;
        if (postContent != null ? !postContent.equals(that.postContent) : that.postContent != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (postId ^ (postId >>> 32));
        result = 31 * result + (postContent != null ? postContent.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery2PersonPostsResult{" +
                "postId=" + postId +
                ", postContent='" + postContent + '\'' +
                '}';
    }
}
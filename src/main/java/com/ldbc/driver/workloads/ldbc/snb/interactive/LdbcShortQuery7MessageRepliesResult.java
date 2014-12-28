package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery7MessageRepliesResult {
    private final long commentId;
    private final String commentContent;

    public LdbcShortQuery7MessageRepliesResult(long commentId, String commentContent) {
        this.commentId = commentId;
        this.commentContent = commentContent;
    }

    public long commentId() {
        return commentId;
    }

    public String commentContent() {
        return commentContent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery7MessageRepliesResult that = (LdbcShortQuery7MessageRepliesResult) o;

        if (commentId != that.commentId) return false;
        if (commentContent != null ? !commentContent.equals(that.commentContent) : that.commentContent != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (commentId ^ (commentId >>> 32));
        result = 31 * result + (commentContent != null ? commentContent.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery7MessageRepliesResult{" +
                "commentId=" + commentId +
                ", commentContent='" + commentContent + '\'' +
                '}';
    }
}
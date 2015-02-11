package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery7MessageRepliesResult {
    private final long commentId;
    private final String commentContent;
    private final long commentCreationDate;
    private final long replyAuthorId;
    private final String replyAuthorFirstName;
    private final String replyAuthorLastName;
    private final boolean replyAuthorKnowsOriginalMessageAuthor;

    public LdbcShortQuery7MessageRepliesResult(long commentId, String commentContent, long commentCreationDate, long replyAuthorId, String replyAuthorFirstName, String replyAuthorLastName, boolean replyAuthorKnowsOriginalMessageAuthor) {
        this.commentId = commentId;
        this.commentContent = commentContent;
        this.commentCreationDate = commentCreationDate;
        this.replyAuthorId = replyAuthorId;
        this.replyAuthorFirstName = replyAuthorFirstName;
        this.replyAuthorLastName = replyAuthorLastName;
        this.replyAuthorKnowsOriginalMessageAuthor = replyAuthorKnowsOriginalMessageAuthor;
    }

    public long commentId() {
        return commentId;
    }

    public String commentContent() {
        return commentContent;
    }

    public long commentCreationDate() {
        return commentCreationDate;
    }

    public long replyAuthorId() {
        return replyAuthorId;
    }

    public String replyAuthorFirstName() {
        return replyAuthorFirstName;
    }

    public String replyAuthorLastName() {
        return replyAuthorLastName;
    }

    public boolean isReplyAuthorKnowsOriginalMessageAuthor() {
        return replyAuthorKnowsOriginalMessageAuthor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery7MessageRepliesResult that = (LdbcShortQuery7MessageRepliesResult) o;

        if (commentCreationDate != that.commentCreationDate) return false;
        if (commentId != that.commentId) return false;
        if (replyAuthorId != that.replyAuthorId) return false;
        if (replyAuthorKnowsOriginalMessageAuthor != that.replyAuthorKnowsOriginalMessageAuthor) return false;
        if (commentContent != null ? !commentContent.equals(that.commentContent) : that.commentContent != null)
            return false;
        if (replyAuthorFirstName != null ? !replyAuthorFirstName.equals(that.replyAuthorFirstName) : that.replyAuthorFirstName != null)
            return false;
        if (replyAuthorLastName != null ? !replyAuthorLastName.equals(that.replyAuthorLastName) : that.replyAuthorLastName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (commentId ^ (commentId >>> 32));
        result = 31 * result + (commentContent != null ? commentContent.hashCode() : 0);
        result = 31 * result + (int) (commentCreationDate ^ (commentCreationDate >>> 32));
        result = 31 * result + (int) (replyAuthorId ^ (replyAuthorId >>> 32));
        result = 31 * result + (replyAuthorFirstName != null ? replyAuthorFirstName.hashCode() : 0);
        result = 31 * result + (replyAuthorLastName != null ? replyAuthorLastName.hashCode() : 0);
        result = 31 * result + (replyAuthorKnowsOriginalMessageAuthor ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery7MessageRepliesResult{" +
                "commentId=" + commentId +
                ", commentContent='" + commentContent + '\'' +
                ", commentCreationDate=" + commentCreationDate +
                ", replyAuthorId=" + replyAuthorId +
                ", replyAuthorFirstName='" + replyAuthorFirstName + '\'' +
                ", replyAuthorLastName='" + replyAuthorLastName + '\'' +
                ", replyAuthorKnowsOriginalMessageAuthor=" + replyAuthorKnowsOriginalMessageAuthor +
                '}';
    }
}
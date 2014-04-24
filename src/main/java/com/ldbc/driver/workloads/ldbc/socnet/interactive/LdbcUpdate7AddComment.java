package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Arrays;
import java.util.Date;

public class LdbcUpdate7AddComment extends Operation<Object> {
    private final long commentId;
    private final Date creationDate;
    private final String locationIp;
    private final String browserUsed;
    private final String content;
    private final int length;
    private final long authorPersonId;
    private final long countryId;
    private final long replyToPostId;
    private final long replyToCommentId;
    private final long[] tagIds;

    public LdbcUpdate7AddComment(long commentId,
                                 Date creationDate,
                                 String locationIp,
                                 String browserUsed,
                                 String content,
                                 int length,
                                 long authorPersonId,
                                 long countryId,
                                 long replyToPostId,
                                 long replyToCommentId,
                                 long[] tagIds) {
        this.commentId = commentId;
        this.creationDate = creationDate;
        this.locationIp = locationIp;
        this.browserUsed = browserUsed;
        this.content = content;
        this.length = length;
        this.authorPersonId = authorPersonId;
        this.countryId = countryId;
        this.replyToPostId = replyToPostId;
        this.replyToCommentId = replyToCommentId;
        this.tagIds = tagIds;
    }

    public long commentId() {
        return commentId;
    }

    public Date creationDate() {
        return creationDate;
    }

    public String locationIp() {
        return locationIp;
    }

    public String browserUsed() {
        return browserUsed;
    }

    public String content() {
        return content;
    }

    public int length() {
        return length;
    }

    public long authorPersonId() {
        return authorPersonId;
    }

    public long countryId() {
        return countryId;
    }

    public long replyToPostId() {
        return replyToPostId;
    }

    public long replyToCommentId() {
        return replyToCommentId;
    }

    public long[] tagIds() {
        return tagIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcUpdate7AddComment that = (LdbcUpdate7AddComment) o;

        if (authorPersonId != that.authorPersonId) return false;
        if (commentId != that.commentId) return false;
        if (countryId != that.countryId) return false;
        if (length != that.length) return false;
        if (replyToCommentId != that.replyToCommentId) return false;
        if (replyToPostId != that.replyToPostId) return false;
        if (browserUsed != null ? !browserUsed.equals(that.browserUsed) : that.browserUsed != null) return false;
        if (content != null ? !content.equals(that.content) : that.content != null) return false;
        if (creationDate != null ? !creationDate.equals(that.creationDate) : that.creationDate != null) return false;
        if (locationIp != null ? !locationIp.equals(that.locationIp) : that.locationIp != null) return false;
        if (!Arrays.equals(tagIds, that.tagIds)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (commentId ^ (commentId >>> 32));
        result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
        result = 31 * result + (locationIp != null ? locationIp.hashCode() : 0);
        result = 31 * result + (browserUsed != null ? browserUsed.hashCode() : 0);
        result = 31 * result + (content != null ? content.hashCode() : 0);
        result = 31 * result + length;
        result = 31 * result + (int) (authorPersonId ^ (authorPersonId >>> 32));
        result = 31 * result + (int) (countryId ^ (countryId >>> 32));
        result = 31 * result + (int) (replyToPostId ^ (replyToPostId >>> 32));
        result = 31 * result + (int) (replyToCommentId ^ (replyToCommentId >>> 32));
        result = 31 * result + (tagIds != null ? Arrays.hashCode(tagIds) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcUpdate7AddComment{" +
                "commentId=" + commentId +
                ", creationDate=" + creationDate +
                ", locationIp='" + locationIp + '\'' +
                ", browserUsed='" + browserUsed + '\'' +
                ", content='" + content + '\'' +
                ", length=" + length +
                ", authorPersonId=" + authorPersonId +
                ", countryId=" + countryId +
                ", replyToPostId=" + replyToPostId +
                ", replyToCommentId=" + replyToCommentId +
                ", tagIds=" + Arrays.toString(tagIds) +
                '}';
    }
}

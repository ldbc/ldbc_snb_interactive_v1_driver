package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery6MessageForumResult {
    private final long forumId;
    private final String forumTitle;
    private final long moderatorId;
    private final String moderatorFirstName;
    private final String moderatorLastName;

    public LdbcShortQuery6MessageForumResult(long forumId, String forumTitle, long moderatorId, String moderatorFirstName, String moderatorLastName) {
        this.forumId = forumId;
        this.forumTitle = forumTitle;
        this.moderatorId = moderatorId;
        this.moderatorFirstName = moderatorFirstName;
        this.moderatorLastName = moderatorLastName;
    }

    public long forumId() {
        return forumId;
    }

    public String forumTitle() {
        return forumTitle;
    }

    public long moderatorId() {
        return moderatorId;
    }

    public String moderatorFirstName() {
        return moderatorFirstName;
    }

    public String moderatorLastName() {
        return moderatorLastName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery6MessageForumResult that = (LdbcShortQuery6MessageForumResult) o;

        if (forumId != that.forumId) return false;
        if (moderatorId != that.moderatorId) return false;
        if (forumTitle != null ? !forumTitle.equals(that.forumTitle) : that.forumTitle != null) return false;
        if (moderatorFirstName != null ? !moderatorFirstName.equals(that.moderatorFirstName) : that.moderatorFirstName != null)
            return false;
        if (moderatorLastName != null ? !moderatorLastName.equals(that.moderatorLastName) : that.moderatorLastName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (forumId ^ (forumId >>> 32));
        result = 31 * result + (forumTitle != null ? forumTitle.hashCode() : 0);
        result = 31 * result + (int) (moderatorId ^ (moderatorId >>> 32));
        result = 31 * result + (moderatorFirstName != null ? moderatorFirstName.hashCode() : 0);
        result = 31 * result + (moderatorLastName != null ? moderatorLastName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery6MessageForumResult{" +
                "forumId=" + forumId +
                ", forumTitle='" + forumTitle + '\'' +
                ", moderatorId=" + moderatorId +
                ", moderatorFirstName='" + moderatorFirstName + '\'' +
                ", moderatorLastName='" + moderatorLastName + '\'' +
                '}';
    }
}
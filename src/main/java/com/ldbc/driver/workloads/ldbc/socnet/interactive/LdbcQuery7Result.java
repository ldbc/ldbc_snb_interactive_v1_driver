package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.Date;

public class LdbcQuery7Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final Date likeCreationDate;
    private final boolean isNew;
    private final long postId;
    private final String postContent;
    private final long milliSecondDelay;

    public LdbcQuery7Result(long personId, String personFirstName, String personLastName, Date likeCreationDate, boolean isNew, long postId, String postContent, long milliSecondDelay) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.likeCreationDate = likeCreationDate;
        this.isNew = isNew;
        this.postId = postId;
        this.postContent = postContent;
        this.milliSecondDelay = milliSecondDelay;
    }

    public long personId() {
        return personId;
    }

    public String personFirstName() {
        return personFirstName;
    }

    public String personLastName() {
        return personLastName;
    }

    public Date likeCreationDate() {
        return likeCreationDate;
    }

    public boolean isNew() {
        return isNew;
    }

    public long postId() {
        return postId;
    }

    public String postContent() {
        return postContent;
    }

    public long milliSecondDelay() {
        return milliSecondDelay;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery7Result that = (LdbcQuery7Result) o;

        if (isNew != that.isNew) return false;
        if (milliSecondDelay != that.milliSecondDelay) return false;
        if (personId != that.personId) return false;
        if (postId != that.postId) return false;
        if (postContent != null ? !postContent.equals(that.postContent) : that.postContent != null) return false;
        if (likeCreationDate != null ? !likeCreationDate.equals(that.likeCreationDate) : that.likeCreationDate != null)
            return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (likeCreationDate != null ? likeCreationDate.hashCode() : 0);
        result = 31 * result + (isNew ? 1 : 0);
        result = 31 * result + (int) (postId ^ (postId >>> 32));
        result = 31 * result + (postContent != null ? postContent.hashCode() : 0);
        result = 31 * result + (int) (milliSecondDelay ^ (milliSecondDelay >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery7Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", likeCreationDate=" + likeCreationDate +
                ", isNew=" + isNew +
                ", postId=" + postId +
                ", postContent='" + postContent + '\'' +
                ", milliSecondDelay=" + milliSecondDelay +
                '}';
    }
}

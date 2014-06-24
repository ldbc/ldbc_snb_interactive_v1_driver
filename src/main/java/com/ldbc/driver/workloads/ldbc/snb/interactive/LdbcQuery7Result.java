package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery7Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long likeCreationDate;
    private final long commentOrPostId;
    private final String commentOrPostContent;
    private final int minutesLatency;
    private final boolean isNew;

    public LdbcQuery7Result(long personId, String personFirstName, String personLastName, long likeCreationDate, long commentOrPostId, String commentOrPostContent, int minutesLatency, boolean isNew) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.likeCreationDate = likeCreationDate;
        this.commentOrPostId = commentOrPostId;
        this.commentOrPostContent = commentOrPostContent;
        this.minutesLatency = minutesLatency;
        this.isNew = isNew;
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

    public long likeCreationDate() {
        return likeCreationDate;
    }

    public long commentOrPostId() {
        return commentOrPostId;
    }

    public String commentOrPostContent() {
        return commentOrPostContent;
    }

    public int minutesLatency() {
        return minutesLatency;
    }

    public boolean isNew() {
        return isNew;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery7Result that = (LdbcQuery7Result) o;

        if (commentOrPostId != that.commentOrPostId) return false;
        if (isNew != that.isNew) return false;
        if (likeCreationDate != that.likeCreationDate) return false;
        if (minutesLatency != that.minutesLatency) return false;
        if (personId != that.personId) return false;
        if (commentOrPostContent != null ? !commentOrPostContent.equals(that.commentOrPostContent) : that.commentOrPostContent != null)
            return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery7Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", likeCreationDate=" + likeCreationDate +
                ", commentOrPostId=" + commentOrPostId +
                ", commentOrPostContent='" + commentOrPostContent + '\'' +
                ", minutesLatency=" + minutesLatency +
                ", isNew=" + isNew +
                '}';
    }
}
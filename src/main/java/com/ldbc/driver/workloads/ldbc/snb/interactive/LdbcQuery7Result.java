package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery7Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long likeCreationDate;
    private final long commentOrPostId;
    private final String commentOrPostContent;
    private final long minutesLatency;
    private final boolean isNew;

    public LdbcQuery7Result(long personId, String personFirstName, String personLastName, long likeCreationDate, long commentOrPostId, String commentOrPostContent, long minutesLatency, boolean isNew) {
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

    public long minutesLatency() {
        return minutesLatency;
    }

    public boolean isNew() {
        return isNew;
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
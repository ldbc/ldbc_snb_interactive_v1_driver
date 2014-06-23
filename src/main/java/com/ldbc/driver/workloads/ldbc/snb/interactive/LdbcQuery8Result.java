package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery8Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long commentCreationDate;
    private final long commentId;
    private final String commentContent;

    public LdbcQuery8Result(long personId, String personFirstName, String personLastName, long commentCreationDate, long commentId, String commentContent) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.commentCreationDate = commentCreationDate;
        this.commentId = commentId;
        this.commentContent = commentContent;
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

    public long commentCreationDate() {
        return commentCreationDate;
    }

    public long commentId() {
        return commentId;
    }

    public String commentContent() {
        return commentContent;
    }

    @Override
    public String toString() {
        return "LdbcQuery8Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", commentCreationDate=" + commentCreationDate +
                ", commentId=" + commentId +
                ", commentContent='" + commentContent + '\'' +
                '}';
    }
}
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery8Result that = (LdbcQuery8Result) o;

        if (commentCreationDate != that.commentCreationDate) return false;
        if (commentId != that.commentId) return false;
        if (personId != that.personId) return false;
        if (commentContent != null ? !commentContent.equals(that.commentContent) : that.commentContent != null)
            return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;

        return true;
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
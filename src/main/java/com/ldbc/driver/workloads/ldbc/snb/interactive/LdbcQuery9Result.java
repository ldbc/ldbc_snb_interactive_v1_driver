package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery9Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long commentOrPostId;
    private final String commentOrPostContent;
    private final long commentOrPostCreationDate;

    public LdbcQuery9Result(long personId, String personFirstName, String personLastName, long commentOrPostId, String commentOrPostContent, long commentOrPostCreationDate) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.commentOrPostId = commentOrPostId;
        this.commentOrPostContent = commentOrPostContent;
        this.commentOrPostCreationDate = commentOrPostCreationDate;
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

    public long commentOrPostId() {
        return commentOrPostId;
    }

    public String commentOrPostContent() {
        return commentOrPostContent;
    }

    public long commentOrPostCreationDate() {
        return commentOrPostCreationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery9Result that = (LdbcQuery9Result) o;

        if (commentOrPostCreationDate != that.commentOrPostCreationDate) return false;
        if (commentOrPostId != that.commentOrPostId) return false;
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
        return "LdbcQuery9Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", commentOrPostId=" + commentOrPostId +
                ", commentOrPostContent='" + commentOrPostContent + '\'' +
                ", commentOrPostCreationDate=" + commentOrPostCreationDate +
                '}';
    }
}
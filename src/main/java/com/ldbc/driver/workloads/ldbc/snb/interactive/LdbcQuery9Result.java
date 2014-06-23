package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery9Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long commentOrPostId;
    private final String commentOrPostContent;
    private final long commentOrPostIdCreationDate;

    public LdbcQuery9Result(long personId, String personFirstName, String personLastName, long commentOrPostId, String commentOrPostContent, long commentOrPostIdCreationDate) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.commentOrPostId = commentOrPostId;
        this.commentOrPostContent = commentOrPostContent;
        this.commentOrPostIdCreationDate = commentOrPostIdCreationDate;
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

    public long commentOrPostIdCreationDate() {
        return commentOrPostIdCreationDate;
    }

    @Override
    public String toString() {
        return "LdbcQuery9Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", commentOrPostId=" + commentOrPostId +
                ", commentOrPostContent='" + commentOrPostContent + '\'' +
                ", commentOrPostIdCreationDate=" + commentOrPostIdCreationDate +
                '}';
    }
}
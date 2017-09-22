package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcQuery13Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long postOrCommentId;
    private final String postOrCommentContent;
    private final long postOrCommentCreationDate;

    public LdbcQuery13Result(long personId, String personFirstName, String personLastName, long postOrCommentId, String postOrCommentContent, long postOrCommentCreationDate) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.postOrCommentId = postOrCommentId;
        this.postOrCommentContent = postOrCommentContent;
        this.postOrCommentCreationDate = postOrCommentCreationDate;
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

    public long postOrCommentId() {
        return postOrCommentId;
    }

    public String postOrCommentContent() {
        return postOrCommentContent;
    }

    public long postOrCommentCreationDate() {
        return postOrCommentCreationDate;
    }

    @Override
    public boolean equals(Object o) {
	if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery13Result result = (LdbcQuery13Result) o;

        if (personId != result.personId) return false;
        if (postOrCommentCreationDate != result.postOrCommentCreationDate) return false;
        if (postOrCommentId != result.postOrCommentId) return false;
        if (personFirstName != null ? !personFirstName.equals(result.personFirstName) : result.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(result.personLastName) : result.personLastName != null)
            return false;
        if (postOrCommentContent != null ? !postOrCommentContent.equals(result.postOrCommentContent) : result.postOrCommentContent != null)
            return false;

        return true;
    }

    @Override
    public String toString() {
	        return "LdbcQuery13Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", postOrCommentId=" + postOrCommentId +
                ", postOrCommentContent='" + postOrCommentContent + '\'' +
                ", postOrCommentCreationDate=" + postOrCommentCreationDate +
                '}';
    }
}

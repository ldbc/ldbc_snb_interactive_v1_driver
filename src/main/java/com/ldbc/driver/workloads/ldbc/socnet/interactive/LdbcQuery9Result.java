package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery9Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long postOrCommentId;
    private final String postOrCommentContent;
    private final long postOrCommentCreationDate;

    public LdbcQuery9Result(long personId, String personFirstName, String personLastName, long postOrCommentId, String postOrCommentContent, long postOrCommentCreationDate) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.postOrCommentId = postOrCommentId;
        this.postOrCommentContent = postOrCommentContent;
        this.postOrCommentCreationDate = postOrCommentCreationDate;
    }

    public long getPersonId() {
        return personId;
    }

    public String getPersonFirstName() {
        return personFirstName;
    }

    public String getPersonLastName() {
        return personLastName;
    }

    public long getPostOrCommentId() {
        return postOrCommentId;
    }

    public String getPostOrCommentContent() {
        return postOrCommentContent;
    }

    public long getPostOrCommentCreationDate() {
        return postOrCommentCreationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery9Result that = (LdbcQuery9Result) o;

        if (personId != that.personId) return false;
        if (postOrCommentCreationDate != that.postOrCommentCreationDate) return false;
        if (postOrCommentId != that.postOrCommentId) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;
        if (postOrCommentContent != null ? !postOrCommentContent.equals(that.postOrCommentContent) : that.postOrCommentContent != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (int) (postOrCommentId ^ (postOrCommentId >>> 32));
        result = 31 * result + (postOrCommentContent != null ? postOrCommentContent.hashCode() : 0);
        result = 31 * result + (int) (postOrCommentCreationDate ^ (postOrCommentCreationDate >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery9Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", postOrCommentId=" + postOrCommentId +
                ", postOrCommentContent='" + postOrCommentContent + '\'' +
                ", postOrCommentCreationDate=" + postOrCommentCreationDate +
                '}';
    }
}

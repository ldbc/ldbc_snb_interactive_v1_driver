package com.ldbc.driver.workloads.ldbc.socnet.interactive;

public class LdbcQuery8Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long replyCreationDate;
    private final long replyId;
    private final String replyContent;

    public LdbcQuery8Result(long personId, String personFirstName, String personLastName, long replyCreationDate, long replyId, String replyContent) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.replyCreationDate = replyCreationDate;
        this.replyId = replyId;
        this.replyContent = replyContent;
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

    public long replyCreationDate() {
        return replyCreationDate;
    }

    public long replyId() {
        return replyId;
    }

    public String replyContent() {
        return replyContent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery8Result that = (LdbcQuery8Result) o;

        if (personId != that.personId) return false;
        if (replyCreationDate != that.replyCreationDate) return false;
        if (replyId != that.replyId) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;
        if (replyContent != null ? !replyContent.equals(that.replyContent) : that.replyContent != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personFirstName != null ? personFirstName.hashCode() : 0);
        result = 31 * result + (personLastName != null ? personLastName.hashCode() : 0);
        result = 31 * result + (int) (replyCreationDate ^ (replyCreationDate >>> 32));
        result = 31 * result + (int) (replyId ^ (replyId >>> 32));
        result = 31 * result + (replyContent != null ? replyContent.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery8Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", replyCreationDate=" + replyCreationDate +
                ", replyId=" + replyId +
                ", replyContent='" + replyContent + '\'' +
                '}';
    }
}

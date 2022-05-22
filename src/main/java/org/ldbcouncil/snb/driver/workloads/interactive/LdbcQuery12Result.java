package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.ldbcouncil.snb.driver.util.ListUtils;

public class LdbcQuery12Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final Iterable<String> tagNames;
    private final int replyCount;

    public LdbcQuery12Result(
        @JsonProperty("personId")        long personId,
        @JsonProperty("personFirstName") String personFirstName,
        @JsonProperty("personLastName")  String personLastName,
        @JsonProperty("tagNames")        Iterable<String> tagNames,
        @JsonProperty("replyCount")      int replyCount
    )
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.tagNames = tagNames;
        this.replyCount = replyCount;
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

    public Iterable<String> getTagNames() {
        return tagNames;
    }

    public int getReplyCount() {
        return replyCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery12Result that = (LdbcQuery12Result) o;

        if (personId != that.personId) return false;
        if (replyCount != that.replyCount) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;
        if (tagNames != null ? !ListUtils.listsEqual(tagNames, that.tagNames) : that.tagNames != null)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery12Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", tagNames=" + tagNames +
                ", replyCount=" + replyCount +
                '}';
    }
}

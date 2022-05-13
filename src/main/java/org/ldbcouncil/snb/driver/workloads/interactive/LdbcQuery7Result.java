package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcQuery7Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long likeCreationDate;
    private final long messageId;
    private final String messageContent;
    private final int minutesLatency;
    private final boolean isNew;

    public LdbcQuery7Result(
        @JsonProperty("personId") long personId,
        @JsonProperty("personFirstName") String personFirstName,
        @JsonProperty("personLastName") String personLastName,
        @JsonProperty("likeCreationDate") long likeCreationDate,
        @JsonProperty("messageId") long messageId,
        @JsonProperty("messageContent") String messageContent,
        @JsonProperty("minutesLatency") int minutesLatency,
        @JsonProperty("isNew") boolean isNew
    ) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.likeCreationDate = likeCreationDate;
        this.messageId = messageId;
        this.messageContent = messageContent;
        this.minutesLatency = minutesLatency;
        this.isNew = isNew;
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

    public long getLikeCreationDate() {
        return likeCreationDate;
    }

    public long getMessageId() {
        return messageId;
    }

    public String getMessageContent() {
        return messageContent;
    }

    public int getMinutesLatency() {
        return minutesLatency;
    }

    public boolean getIsNew() {
        return isNew;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery7Result that = (LdbcQuery7Result) o;

        if (messageId != that.messageId) return false;
        if (isNew != that.isNew) return false;
        if (likeCreationDate != that.likeCreationDate) return false;
        if (minutesLatency != that.minutesLatency) return false;
        if (personId != that.personId) return false;
        if (messageContent != null ? !messageContent.equals(that.messageContent) : that.messageContent != null)
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
                ", messageId=" + messageId +
                ", messageContent='" + messageContent + '\'' +
                ", minutesLatency=" + minutesLatency +
                ", isNew=" + isNew +
                '}';
    }
}

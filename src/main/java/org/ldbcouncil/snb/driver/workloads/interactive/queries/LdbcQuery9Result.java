package org.ldbcouncil.snb.driver.workloads.interactive.queries;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcQuery9Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long messageId;
    private final String messageContent;
    private final long messageCreationDate;

    public LdbcQuery9Result(
        @JsonProperty("personId")            long personId,
        @JsonProperty("personFirstName")     String personFirstName,
        @JsonProperty("personLastName")      String personLastName,
        @JsonProperty("messageId")           long messageId,
        @JsonProperty("messageContent")      String messageContent,
        @JsonProperty("messageCreationDate") long messageCreationDate
    )
    {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.messageId = messageId;
        this.messageContent = messageContent;
        this.messageCreationDate = messageCreationDate;
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

    public long getMessageId() {
        return messageId;
    }

    public String getMessageContent() {
        return messageContent;
    }

    public long getMessageCreationDate() {
        return messageCreationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery9Result that = (LdbcQuery9Result) o;

        if (messageCreationDate != that.messageCreationDate) return false;
        if (messageId != that.messageId) return false;
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
        return "LdbcQuery9Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", messageId=" + messageId +
                ", messageContent='" + messageContent + '\'' +
                ", messageCreationDate=" + messageCreationDate +
                '}';
    }
}

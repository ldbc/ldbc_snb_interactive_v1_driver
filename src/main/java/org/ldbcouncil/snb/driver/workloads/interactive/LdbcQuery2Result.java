package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcQuery2Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final long messageId;
    private final String messageContent;
    private final long messageCreationDate;

    public LdbcQuery2Result(
        @JsonProperty("personId") long personId,
        @JsonProperty("personFirstName") String personFirstName,
        @JsonProperty("personLastName") String personLastName, 
        @JsonProperty("messageId") long messageId,
        @JsonProperty("messageContent") String messageContent,
        @JsonProperty("messageCreationDate") long messageCreationDate
    ) {
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

        LdbcQuery2Result result = (LdbcQuery2Result) o;

        if (personId != result.personId) return false;
        if (messageCreationDate != result.messageCreationDate) return false;
        if (messageId != result.messageId) return false;
        if (personFirstName != null ? !personFirstName.equals(result.personFirstName) : result.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(result.personLastName) : result.personLastName != null)
            return false;
        if (messageContent != null ? !messageContent.equals(result.messageContent) : result.messageContent != null)
            return false;

        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery2Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", messageId=" + messageId +
                ", messageContent='" + messageContent + '\'' +
                ", messageCreationDate=" + messageCreationDate +
                '}';
    }
}

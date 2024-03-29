package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LdbcShortQuery2PersonPostsResult {
    private final long messageId;
    private final String messageContent;
    private final long messageCreationDate;
    private final long originalPostId;
    private final long originalPostAuthorId;
    private final String originalPostAuthorFirstName;
    private final String originalPostAuthorLastName;

    public LdbcShortQuery2PersonPostsResult(
        @JsonProperty("messageId")                   long messageId,
        @JsonProperty("messageContent")              String messageContent,
        @JsonProperty("messageCreationDate")         long messageCreationDate,
        @JsonProperty("originalPostId")              long originalPostId,
        @JsonProperty("originalPostAuthorId")        long originalPostAuthorId,
        @JsonProperty("originalPostAuthorFirstName") String originalPostAuthorFirstName,
        @JsonProperty("originalPostAuthorLastName")  String originalPostAuthorLastName
    )
    {
        this.messageId = messageId;
        this.messageContent = messageContent;
        this.messageCreationDate = messageCreationDate;
        this.originalPostId = originalPostId;
        this.originalPostAuthorId = originalPostAuthorId;
        this.originalPostAuthorFirstName = originalPostAuthorFirstName;
        this.originalPostAuthorLastName = originalPostAuthorLastName;
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

    public long getOriginalPostId() {
        return originalPostId;
    }

    public long getOriginalPostAuthorId() {
        return originalPostAuthorId;
    }

    public String getOriginalPostAuthorFirstName() {
        return originalPostAuthorFirstName;
    }

    public String getOriginalPostAuthorLastName() {
        return originalPostAuthorLastName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery2PersonPostsResult that = (LdbcShortQuery2PersonPostsResult) o;

        if (messageCreationDate != that.messageCreationDate) return false;
        if (messageId != that.messageId) return false;
        if (originalPostAuthorId != that.originalPostAuthorId) return false;
        if (originalPostId != that.originalPostId) return false;
        if (messageContent != null ? !messageContent.equals(that.messageContent) : that.messageContent != null)
            return false;
        if (originalPostAuthorFirstName != null ? !originalPostAuthorFirstName.equals(that.originalPostAuthorFirstName) : that.originalPostAuthorFirstName != null)
            return false;
        if (originalPostAuthorLastName != null ? !originalPostAuthorLastName.equals(that.originalPostAuthorLastName) : that.originalPostAuthorLastName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (messageId ^ (messageId >>> 32));
        result = 31 * result + (messageContent != null ? messageContent.hashCode() : 0);
        result = 31 * result + (int) (messageCreationDate ^ (messageCreationDate >>> 32));
        result = 31 * result + (int) (originalPostId ^ (originalPostId >>> 32));
        result = 31 * result + (int) (originalPostAuthorId ^ (originalPostAuthorId >>> 32));
        result = 31 * result + (originalPostAuthorFirstName != null ? originalPostAuthorFirstName.hashCode() : 0);
        result = 31 * result + (originalPostAuthorLastName != null ? originalPostAuthorLastName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery2PersonPostsResult{" +
                "messageId=" + messageId +
                ", messageContent='" + messageContent + '\'' +
                ", messageCreationDate=" + messageCreationDate +
                ", originalPostId=" + originalPostId +
                ", originalPostAuthorId=" + originalPostAuthorId +
                ", originalPostAuthorFirstName='" + originalPostAuthorFirstName + '\'' +
                ", originalPostAuthorLastName='" + originalPostAuthorLastName + '\'' +
                '}';
    }
}

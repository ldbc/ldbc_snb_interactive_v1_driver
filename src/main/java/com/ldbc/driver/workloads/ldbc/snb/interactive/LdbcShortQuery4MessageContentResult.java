package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery4MessageContentResult {
    private final String messageContent;
    private final long messageCreationDate;

    public LdbcShortQuery4MessageContentResult(String messageContent, long messageCreationDate) {
        this.messageContent = messageContent;
        this.messageCreationDate = messageCreationDate;
    }

    public String messageContent() {
        return messageContent;
    }

    public long messageCreationDate() {
        return messageCreationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery4MessageContentResult that = (LdbcShortQuery4MessageContentResult) o;

        if (messageCreationDate != that.messageCreationDate) return false;
        if (messageContent != null ? !messageContent.equals(that.messageContent) : that.messageContent != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = messageContent != null ? messageContent.hashCode() : 0;
        result = 31 * result + (int) (messageCreationDate ^ (messageCreationDate >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery4MessageContentResult{" +
                "messageContent='" + messageContent + '\'' +
                ", messageCreationDate=" + messageCreationDate +
                '}';
    }
}
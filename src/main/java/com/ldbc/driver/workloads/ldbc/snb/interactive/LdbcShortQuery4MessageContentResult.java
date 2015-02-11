package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery4MessageContentResult {
    private final long messageId;
    private final String messageContent;

    public LdbcShortQuery4MessageContentResult(long messageId, String messageContent) {
        this.messageId = messageId;
        this.messageContent = messageContent;
    }

    public long messageId() {
        return messageId;
    }

    public String messageContent() {
        return messageContent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery4MessageContentResult that = (LdbcShortQuery4MessageContentResult) o;

        if (messageId != that.messageId) return false;
        if (messageContent != null ? !messageContent.equals(that.messageContent) : that.messageContent != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (messageId ^ (messageId >>> 32));
        result = 31 * result + (messageContent != null ? messageContent.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery4MessageContentResult{" +
                "messageId=" + messageId +
                ", messageContent='" + messageContent + '\'' +
                '}';
    }
}
package com.ldbc.driver.workloads.ldbc.snb.interactive;

public class LdbcShortQuery4MessageContentResult {
    private final String messageContent;

    public LdbcShortQuery4MessageContentResult(String messageContent) {
        this.messageContent = messageContent;
    }

    public String messageContent() {
        return messageContent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcShortQuery4MessageContentResult that = (LdbcShortQuery4MessageContentResult) o;

        if (messageContent != null ? !messageContent.equals(that.messageContent) : that.messageContent != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return messageContent != null ? messageContent.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "LdbcShortQuery4MessageContentResult{" +
                "messageContent='" + messageContent + '\'' +
                '}';
    }
}
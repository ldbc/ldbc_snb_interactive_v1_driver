package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.util.Objects;

public class LdbcSnbBiQuery16FakeNewsDetectionResult
{
    private final long personId;
    private final int messageCountA;
    private final int messageCountB;

    public LdbcSnbBiQuery16FakeNewsDetectionResult( long personId, int messageCountA, int messageCountB )
    {
        this.personId = personId;
        this.messageCountA = messageCountA;
        this.messageCountB = messageCountB;
    }

    public long personId() {
        return personId;
    }

    public int messageCountA() {
        return messageCountA;
    }

    public int messageCountB() {
        return messageCountB;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery16FakeNewsDetectionResult{" +
                "personId=" + personId +
                ", messageCountA=" + messageCountA +
                ", messageCountB=" + messageCountB +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery16FakeNewsDetectionResult that = (LdbcSnbBiQuery16FakeNewsDetectionResult) o;

        if (personId != that.personId) return false;
        if (messageCountA != that.messageCountA) return false;
        return messageCountB == that.messageCountB;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + messageCountA;
        result = 31 * result + messageCountB;
        return result;
    }
}

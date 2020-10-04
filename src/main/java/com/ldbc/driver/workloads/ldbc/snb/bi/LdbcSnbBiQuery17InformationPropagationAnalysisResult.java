package com.ldbc.driver.workloads.ldbc.snb.bi;

import java.util.Objects;

public class LdbcSnbBiQuery17InformationPropagationAnalysisResult
{
    private final long person1Id;
    private final int messageCount;

    public LdbcSnbBiQuery17InformationPropagationAnalysisResult( long person1Id, int messageCount )
    {
        this.person1Id = person1Id;
        this.messageCount = messageCount;
    }

    public long person1Id() {
        return person1Id;
    }

    public int messageCount() {
        return messageCount;
    }

    @Override
    public String toString() {
        return "LdbcSnbBiQuery17InformationPropagationAnalysisResult{" +
                "person1Id=" + person1Id +
                ", messageCount=" + messageCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcSnbBiQuery17InformationPropagationAnalysisResult that = (LdbcSnbBiQuery17InformationPropagationAnalysisResult) o;

        if (person1Id != that.person1Id) return false;
        return messageCount == that.messageCount;
    }

    @Override
    public int hashCode() {
        int result = (int) (person1Id ^ (person1Id >>> 32));
        result = 31 * result + messageCount;
        return result;
    }
}

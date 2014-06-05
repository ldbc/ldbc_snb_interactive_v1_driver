package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.List;

public class LdbcQuery12 extends Operation<List<LdbcQuery12Result>> {
    public static final int DEFAULT_LIMIT = 20;

    private final long personId;
    private final String personUri;
    private final String tagClassName;
    private final int limit;

    public LdbcQuery12(long personId, String personUri, String tagClassName, int limit) {
        this.personId = personId;
        this.personUri = personUri;
        this.tagClassName = tagClassName;
        this.limit = limit;
    }

    public long personId() {
        return personId;
    }

    public String personUri() {
        return personUri;
    }

    public String tagClassName() {
        return tagClassName;
    }

    public int limit() {
        return limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery12 that = (LdbcQuery12) o;

        if (limit != that.limit) return false;
        if (personId != that.personId) return false;
        if (personUri != null ? !personUri.equals(that.personUri) : that.personUri != null) return false;
        if (tagClassName != null ? !tagClassName.equals(that.tagClassName) : that.tagClassName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (personId ^ (personId >>> 32));
        result = 31 * result + (personUri != null ? personUri.hashCode() : 0);
        result = 31 * result + (tagClassName != null ? tagClassName.hashCode() : 0);
        result = 31 * result + limit;
        return result;
    }

    @Override
    public String toString() {
        return "LdbcQuery12{" +
                "personId=" + personId +
                ", personUri='" + personUri + '\'' +
                ", tagClassName='" + tagClassName + '\'' +
                ", limit=" + limit +
                '}';
    }
}

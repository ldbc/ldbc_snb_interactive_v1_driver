package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.ldbc.driver.Operation;

import java.util.Date;
import java.util.List;

public class LdbcQuery5 extends Operation<List<LdbcQuery5Result>> {
    private final long personId;
    private final Date joinDate;

    public LdbcQuery5(long personId, Date joinDate) {
        super();
        this.personId = personId;
        this.joinDate = joinDate;
    }

    public long personId() {
        return personId;
    }

    public Date joinDate() {
        return joinDate;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((joinDate == null) ? 0 : joinDate.hashCode());
        result = prime * result + (int) (personId ^ (personId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        LdbcQuery5 other = (LdbcQuery5) obj;
        if (joinDate == null) {
            if (other.joinDate != null) return false;
        } else if (!joinDate.equals(other.joinDate)) return false;
        if (personId != other.personId) return false;
        return true;
    }

    @Override
    public String toString() {
        return "LdbcQuery5 [personId=" + personId + ", joinDate=" + joinDate + "]";
    }
}

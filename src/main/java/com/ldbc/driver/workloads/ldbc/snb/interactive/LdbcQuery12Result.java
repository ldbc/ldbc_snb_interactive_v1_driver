package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.util.ListUtils;

import java.util.Collections;
import java.util.List;

public class LdbcQuery12Result {
    private final long personId;
    private final String personFirstName;
    private final String personLastName;
    private final Iterable<String> tagNames;
    private final int replyCount;

    public LdbcQuery12Result(long personId, String personFirstName, String personLastName, Iterable<String> tagNames, int replyCount) {
        this.personId = personId;
        this.personFirstName = personFirstName;
        this.personLastName = personLastName;
        this.tagNames = tagNames;
        this.replyCount = replyCount;
    }

    public long personId() {
        return personId;
    }

    public String personFirstName() {
        return personFirstName;
    }

    public String personLastName() {
        return personLastName;
    }

    public Iterable<String> tagNames() {
        return tagNames;
    }

    public int replyCount() {
        return replyCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery12Result that = (LdbcQuery12Result) o;

        if (personId != that.personId) return false;
        if (replyCount != that.replyCount) return false;
        if (personFirstName != null ? !personFirstName.equals(that.personFirstName) : that.personFirstName != null)
            return false;
        if (personLastName != null ? !personLastName.equals(that.personLastName) : that.personLastName != null)
            return false;
        if (tagNames != null ? !tagNamesEqual(tagNames, that.tagNames) : that.tagNames != null)
            return false;

        return true;
    }

    private boolean tagNamesEqual(Iterable<String> tagNames1, Iterable<String> tagNames2) {
        if (null == tagNames1 || null == tagNames2) return false;
        return ListUtils.listsEqual(sort(tagNames1), sort(tagNames2));
    }

    private List<String> sort(Iterable<String> iterable) {
        List<String> list = Lists.newArrayList(iterable);
        Collections.sort(list);
        return list;
    }

    @Override
    public String toString() {
        return "LdbcQuery12Result{" +
                "personId=" + personId +
                ", personFirstName='" + personFirstName + '\'' +
                ", personLastName='" + personLastName + '\'' +
                ", tagNames=" + tagNames +
                ", replyCount=" + replyCount +
                '}';
    }
}

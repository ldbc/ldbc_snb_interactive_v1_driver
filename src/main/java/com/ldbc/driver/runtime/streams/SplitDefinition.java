package com.ldbc.driver.runtime.streams;

import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.Set;

public class SplitDefinition<BASE_ITEM_TYPE> {
    private final Set<Class<? extends BASE_ITEM_TYPE>> itemTypes;

    public SplitDefinition(Class<? extends BASE_ITEM_TYPE>... itemTypes) throws IteratorSplittingException {
        if (null == itemTypes || itemTypes.length == 0)
            throw new IteratorSplittingException("Item types can not be null or empty");
        this.itemTypes = Sets.newHashSet(itemTypes);
    }

    boolean overlapsWith(SplitDefinition<BASE_ITEM_TYPE> otherDefinition) {
        Set<Class<? extends BASE_ITEM_TYPE>> intersection = new HashSet<Class<? extends BASE_ITEM_TYPE>>();
        intersection.addAll(itemTypes);
        intersection.retainAll(otherDefinition.itemTypes);
        return intersection.isEmpty() == false;
    }

    Iterable<Class<? extends BASE_ITEM_TYPE>> itemTypes() {
        return itemTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SplitDefinition that = (SplitDefinition) o;

        if (itemTypes != null ? !itemTypes.equals(that.itemTypes) : that.itemTypes != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return itemTypes != null ? itemTypes.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "SplitDefinition{" +
                "itemTypes=" + itemTypes +
                '}';
    }
}
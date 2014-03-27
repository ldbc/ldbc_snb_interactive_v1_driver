package com.ldbc.driver.runtime.streams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitResult<ITEM_TYPE> {
    private final Map<SplitDefinition<ITEM_TYPE>, Iterable<ITEM_TYPE>> splits = new HashMap<SplitDefinition<ITEM_TYPE>, Iterable<ITEM_TYPE>>();

    List<ITEM_TYPE> addSplit(SplitDefinition<ITEM_TYPE> definition) throws IteratorSplittingException {
        for (SplitDefinition<ITEM_TYPE> existingDefinition : splits.keySet()) {
            if (existingDefinition.overlapsWith(definition))
                throw new IteratorSplittingException(String.format(
                        "Definition overlaps with an existing definition\nNew: %s\nExisting: %s",
                        definition.toString(),
                        existingDefinition.toString()));
        }
        List<ITEM_TYPE> splitContainer = new ArrayList<ITEM_TYPE>();
        splits.put(definition, splitContainer);
        return splitContainer;
    }

    public int count() {
        return splits.size();
    }

    public Iterable<ITEM_TYPE> getSplitFor(SplitDefinition definition) throws IteratorSplittingException {
        if (false == splits.containsKey(definition))
            throw new IteratorSplittingException(String.format("No split definition exists for: %s", definition));
        return splits.get(definition);
    }
}

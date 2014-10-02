package com.ldbc.driver.runtime.streams;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Splits an Iterator into multiple Lists
 */
public class IteratorSplitter<ITEM_TYPE> {
    public enum UnmappedItemPolicy {
        // If an item is found that does not have an associated split it is ignored and the next is retrieved
        DROP,
        // If an item is found that does not have an associated split the splitting process is aborted
        ABORT
    }

    private final UnmappedItemPolicy unmappedItemPolicy;

    public IteratorSplitter(UnmappedItemPolicy unmappedItemPolicy) {
        this.unmappedItemPolicy = unmappedItemPolicy;
    }

    /**
     * Registers split definitions with splits, then populates those splits and returns them a split result
     *
     * @param items       original, un-split iterator
     * @param definitions item types to associate with splits
     * @return a SplitResult containing all registered splits
     * @throws IteratorSplittingException if an item type is registered with more than one split
     */
    public SplitResult<ITEM_TYPE> split(Iterator<? extends ITEM_TYPE> items, SplitDefinition<ITEM_TYPE>... definitions) throws IteratorSplittingException {
        Map<Class<? extends ITEM_TYPE>, List<ITEM_TYPE>> splitListsByClass = new HashMap<>();
        SplitResult<ITEM_TYPE> splitResult = new SplitResult<>();
        for (SplitDefinition<ITEM_TYPE> definition : definitions) {
            List<ITEM_TYPE> splitList = splitResult.addSplit(definition);
            for (Class<? extends ITEM_TYPE> itemType : definition.itemTypes()) {
                splitListsByClass.put(itemType, splitList);
            }
        }
        while (items.hasNext()) {
            ITEM_TYPE item = items.next();
            List<ITEM_TYPE> splitListForClass = splitListsByClass.get(item.getClass());
            if (null == splitListForClass) {
                switch (unmappedItemPolicy) {
                    case DROP:
                        continue;
                    case ABORT:
                        throw new IteratorSplittingException(
                                String.format("Item type has no associated split mapping: %s", item.getClass().getSimpleName())
                        );
                    default:
                        throw new IteratorSplittingException(
                                String.format("Unknown UnmappedItemPolicy: %s", unmappedItemPolicy.name())
                        );
                }
            } else {
                splitListForClass.add(item);
            }
        }
        return splitResult;
    }
}
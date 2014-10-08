package com.ldbc.driver.util;

import java.util.List;

public class ListUtils {

    public static <T extends Object> boolean listsOfListsEqual(List<List<T>> listOfLists1, List<List<T>> listOfLists2) {
        if (listOfLists1.size() != listOfLists2.size()) return false;
        for (int i = 0; i < listOfLists1.size(); i++) {
            List<T> list1 = listOfLists1.get(i);
            List<T> list2 = listOfLists2.get(i);
            if (list1.size() != list2.size()) return false;
            for (int j = 0; j < list1.size(); j++) {
                if (false == list1.get(j).toString().equals(list2.get(j).toString())) return false;
            }
        }
        return true;
    }

    public static <T extends Object> boolean listsEqual(List<T> list1, List<T> list2) {
        if (list1.size() != list2.size()) return false;
        for (int i = 0; i < list1.size(); i++) {
            if (false == list1.get(i).equals(list2.get(i))) return false;
        }
        return true;
    }
}

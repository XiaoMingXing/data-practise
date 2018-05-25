package com.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class Combination {

    public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
        List<List<T>> result = new ArrayList<List<T>>();
        if (n == 0) {
            result.add(newArrayList());
            return result;
        }


        List<List<T>> combinations = findSortedCombinations(elements, n - 1);
        for (List<T> combination : combinations) {
            for (T element : elements) {
                if (combination.contains(element)) {
                    continue;
                }
                ArrayList<T> list = newArrayList();
                list.addAll(combination);

                if (list.contains(element)) {
                    continue;
                }
                list.add(element);
                Collections.sort(list);
                if (result.contains(list)) {
                    continue;
                }
                result.add(list);
            }

        }
        return result;
    }

    public static <T extends Comparable<? super T>> List<List<T>>
    findSortedCombinations(Collection<T> elements) {
        List<List<T>> result = newArrayList();
        for (int index = 0; index <= elements.size(); index++) {
            result.addAll(findSortedCombinations(elements, index));
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        ArrayList<String> list = newArrayList("a", "b", "c", "d");
        System.out.println("list=" + list);
        List<List<String>> combinations = findSortedCombinations(list);
        System.out.println(combinations.size());
        System.out.println(combinations);
    }
}

package com.questions.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Solution {

    public static int[] tree(int[] left, int[] right, int[] nodesLeft, int[] nodesRight) {
        // Write your code here
        HashMap<Integer, List<Integer>> map = new HashMap<>();

        for (int index = 0; index < left.length; index++) {
            List<Integer> values = map.get(left[index]);
            if (map.get(left[index]) == null) {
                values = new ArrayList<>();
            }
            values.add(right[index]);
            map.put(left[index], values);
        }

        int[] result = new int[nodesLeft.length];
        for (int index = 0; index < nodesLeft.length; index++) {
            result[index] = relation(map, nodesLeft[index], nodesRight[index]);
        }
        return result;
    }

    private static int relation(HashMap<Integer, List<Integer>> map, int nodeLeft, int nodeRight) {
        boolean isParentRelation = isParentRelationship(map, nodeLeft, nodeRight);
        if (isParentRelation) {
            return 2;
        }
        boolean isSiblingRelation = isSiblingRelationship(map, nodeLeft, nodeRight);
        if (isSiblingRelation) {
            return 1;
        }
        return 0;
    }

    private static boolean isSiblingRelationship(HashMap<Integer, List<Integer>> map, int nodeLeft, int nodeRight) {
        boolean isSibling = false;
        for (Integer key : map.keySet()) {
            List<Integer> values = map.get(key);
            if (values.indexOf(nodeLeft) != -1 && values.indexOf(nodeRight) != -1) {
                isSibling = true;
            }
        }
        return isSibling;
    }

    private static boolean isParentRelationship(HashMap<Integer, List<Integer>> map, int nodeLeft, int nodeRight) {
        List<Integer> values = map.get(nodeLeft);
        if (values == null || values.size() == 0) {
            return false;
        }
        if (values.size() > 1) {
            return values.indexOf(nodeRight) != -1;
        }
        return false;
    }
}

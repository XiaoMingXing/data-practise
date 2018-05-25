package com.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

class CombinationTest {

    @Test
    void shouldReturnCorrectList() {

        List<List<String>> combinations = Combination
                .findSortedCombinations(newArrayList("A", "B", "C", "D"), 2);

        combinations.forEach(System.out::println);

    }
}
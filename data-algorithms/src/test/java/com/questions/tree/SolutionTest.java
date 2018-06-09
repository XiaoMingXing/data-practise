package com.questions.tree;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SolutionTest {


    @Test
    public void shouldReturnTwo() {

        int[] left = {1, 1};
        int[] right = {2, 3};
        int[] relationA = {1, 2};
        int[] relationB = {2, 3};

        int[] result = Solution.tree(left, right, relationA, relationB);
        assertThat(result[0], is(2));
        assertThat(result[1], is(1));
    }

    @Test
    public void scenario2() {

        int[] left = {1, 1, 2};
        int[] right = {2, 3, 4};
        int[] relationA = {1, 2, 1};
        int[] relationB = {2, 3, 4};

        int[] result = Solution.tree(left, right, relationA, relationB);
        for (int res : result) {
            System.out.print(res);
        }
        assertThat(result[0], is(2));
        assertThat(result[1], is(1));
        assertThat(result[2], is(0));
    }
}
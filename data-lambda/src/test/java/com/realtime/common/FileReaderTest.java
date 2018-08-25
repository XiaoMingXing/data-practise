package com.realtime.common;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class FileReaderTest {

    @Test
    public void testFileReaders() throws IOException {

        FileReader fileReader = new FileReader();
        List<String> lines = fileReader.readData(100);
        lines.forEach(System.out::println);
    }

    @Test
    public void testTotalLineNumber() throws IOException {

        FileReader fileReader = new FileReader();
        int totalLineNumber = fileReader.getTotalLineNumber();
        System.out.println(totalLineNumber);
    }

    @Test
    public void testGenerateLineNumbers() {
        FileReader fileReader = new FileReader();
        int[] numbers = fileReader.generateLineNumbers(10, 1001);

        for (int number : numbers) {
            System.out.print(number + ",");
        }

    }
}
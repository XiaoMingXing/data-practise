package com.realtime.common;

import org.bouncycastle.util.Arrays;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.IntStream;

public class FileReader {

    private final static String fileName = "sample_sparkData.csv";
    private String filePath;


    public List<String> readData(int number) throws IOException {

        ArrayList<String> list = new ArrayList<>();

        String filePath = getFilePath();
        if (filePath == null) {
            return list;
        }
        File file = new File(filePath);

        int totalLineNumber = getTotalLineNumber();
        int[] lineNumbers = generateLineNumbers(number, totalLineNumber);
        int count = 0;

        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNext() && list.size() < number) {
                String line = scanner.nextLine();
                count++;
                if (Arrays.contains(lineNumbers, count)) {
                    list.add(line);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return list;
    }


    public int[] generateLineNumbers(int count, int max) {
        Random random = new Random();
        IntStream sorted = random.ints(count, 1, max);
        return sorted.toArray();
    }

    private String getFilePath() {
        if (this.filePath == null) {
            URL resource = getClass().getClassLoader().getResource(fileName);
            if (resource == null) {
                return null;
            }
            this.filePath = resource.getFile();
        }
        return this.filePath;
    }

    int getTotalLineNumber() throws IOException {
        String filePath = this.getFilePath();
        if (filePath == null) {
            return 0;
        }
        InputStream is = new BufferedInputStream(new FileInputStream(filePath));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
}

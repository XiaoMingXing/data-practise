package com.realtime.generator;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogGenerator {

    private static final long SPEED = 1000; // 每秒1000条
    public static final int SEEKING_RANGE = 10000;

    private static Random rand = new Random();

    public static void main(String[] args) {
        long speed = SPEED;
        if (args.length > 0) {
            speed = Long.valueOf(args[0]);
        }
        long delay = 1000_000 / speed; // 每条耗时多少毫秒

        try (InputStream inputStream = LogGenerator.class.getClassLoader().
                getResourceAsStream("sample_data/user_behavior.log")) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            long start = System.nanoTime();
            while (reader.ready()) {
                String line = reader.readLine();
                System.out.println(line);

                long end = System.nanoTime();
                long diff = end - start;
                while (diff < (delay * 1000)) {
                    Thread.sleep(1);
                    end = System.nanoTime();
                    diff = end - start;
                }
                start = end;
            }
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static List readRandomLines(int count) {
        try {

            URL resource = LogGenerator.class.getClassLoader()
                    .getResource("sample_data/user_behavior.log");
            if (resource != null) {
                int start = rand.nextInt(getNumberOfLines(resource.getPath()));
                Stream<String> lines = Files.lines(Paths.get(resource.getPath()));
                return lines
                        .skip(start - 1)
                        .limit(count)
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static int getNumberOfLines(String path) throws IOException {
        try (LineNumberReader reader = new LineNumberReader(new FileReader(path))) {
            reader.skip(Integer.MAX_VALUE);
            return reader.getLineNumber() + 1;
        }
    }
}

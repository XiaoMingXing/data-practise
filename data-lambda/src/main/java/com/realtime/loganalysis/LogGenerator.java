package com.realtime.loganalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;

public class LogGenerator {


    public static void main(String... args) {
        if (args.length != 2) {
            System.out.println("Usage - java LogGenerator <location of log File to read> <Location of log files in which logs need to be updated>");
            System.exit(0);
        }
        try {
            String location = args[0];

            File file = new File(location);
            FileOutputStream writer = new FileOutputStream(file);

            File read = new File(args[1]);
            BufferedReader reader = new BufferedReader(new FileReader(read));

            for (; ; ) {
                writer.write((reader.readLine() + "\n").getBytes());
                writer.flush();
                Thread.sleep(500);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

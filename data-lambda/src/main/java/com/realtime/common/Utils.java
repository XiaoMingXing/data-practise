package com.realtime.common;


import com.opencsv.CSVReader;
import org.apache.commons.lang.StringUtils;


import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;



public class Utils {

    private final static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public static String coalesce(Object value) {
        return value == null ? "" : value.toString();
    }

    public static List<String> parseArray(Object[] values) {
        List<String> result = new ArrayList<>();
        for (int index = 0; index < values.length; index++) {
            result.add(index, coalesce(values[index]));
        }
        return result;
    }

    public static boolean isNullOrEmpty(String url_spm_id) {
        return url_spm_id == null || "".equals(url_spm_id);
    }

    public static List<String[]> readCsv(String fileName) {
        List<String[]> result = new ArrayList<>();
        try {
            URL resource = Utils.class.getClassLoader().getResource(fileName);

            if (resource != null) {
                String path = resource.getPath();
                if (System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
                    path = StringUtils.removeStart(path, "/");
                }
                Reader reader = Files.newBufferedReader(Paths.get(path));
                CSVReader csvReader = new CSVReader(reader);
                result = csvReader.readAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

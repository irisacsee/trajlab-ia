package org.irisacsee.trajlab.util;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.List;

public class TrajFileSegment {
    private static final String INPUT_PATH = "C:\\xc\\Tool\\BigData\\tdrive\\release\\taxi_log_2008_by_id";
    private static final String OUTPUT_PATH = "C:\\xc\\Tool\\BigData\\tdrive\\release\\segment";
    private static final long SEGMENT_INTERVAL = 60 * 60 * 1000;
    private static int tid = 1;

    public static void main(String[] args) throws IOException {
        process();
//        clear();
    }

    private static void process() throws IOException {
        long start = System.currentTimeMillis();
        List<String> fileNames = IOUtil.getFileNames(INPUT_PATH);
        StringBuilder builder = new StringBuilder();
        for (String fileName : fileNames) {
            String[] fileNameSplit = fileName.split("\\\\");
            String name = fileNameSplit[fileNameSplit.length - 1].split("\\.")[0];
            List<String> lines = IOUtil.readCSV(fileName, false);
            if (!lines.isEmpty()) {
                long prev = Timestamp.valueOf(lines.get(0).split(",")[1]).getTime();
//            System.out.println("timeStr: " + timeStr + ", prev: " + prev);
                int len = 0;
                for (String line : lines) {
                    long curr = Timestamp.valueOf(line.split(",")[1]).getTime();
                    if (curr - prev < SEGMENT_INTERVAL) {
                        append(builder, line);
                        ++len;
                    } else if (len == 1) {
                        append(builder, line);
                        prev = curr;
                        ++len;
//                        System.out.println("this tid: " + tid);
                    } else {
                        output(builder, name);
                        append(builder, line);
                        prev = curr;
                        len = 1;
                    }
                }
                if (builder.length() > 0) {
                    output(builder, name);
                }
            } else {
                System.out.println("empty file: " + name);
            }
        }
        System.out.println("cost: " + (System.currentTimeMillis() - start));
        System.out.println("tid: " + tid);
    }

    private static void append(StringBuilder builder, String line) {
        builder.append(tid);
        builder.append(",");
        builder.append(line);
        builder.append("\r\n");
    }

    private static void output(StringBuilder builder, String name) throws IOException {
        IOUtil.writeStringToFile(OUTPUT_PATH + "\\" + name + "_" + tid + ".txt", builder.toString());
        ++tid;
        builder.delete(0, builder.length());
    }

    private static void clear() throws IOException {
        List<String> fileNames = IOUtil.getFileNames(OUTPUT_PATH);
        for (String fileName : fileNames) {
            Files.delete(Paths.get(fileName));
        }
    }
}

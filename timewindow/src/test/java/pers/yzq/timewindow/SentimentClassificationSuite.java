package pers.yzq.timewindow;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.Test;

import java.io.*;

public class SentimentClassificationSuite {

    @Test
    public void generate() throws IOException {
        File file = new File("E:\\Project\\Scala Project\\spark-2.4.4\\timewindow\\src\\test\\resources\\SentiWordNet.txt");
        FileInputStream inputStream = new FileInputStream(file);
        InputStreamReader streamReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(streamReader);
        String line = reader.readLine();
        while (line != null) {
            if (line.charAt(0) != '#') {
                String [] formats = line.split("\t");
                String [] words = formats[4].split(" ");
                for (String word : words) {
                    System.out.println(word);
                }
            }
            line = reader.readLine();
        }
    }
}

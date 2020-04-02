package org.apache.spark.yzq;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;

import java.io.IOException;

public class CustomizedFileAppender extends FileAppender {

    public CustomizedFileAppender() {}

    public CustomizedFileAppender(Layout layout, String filename, boolean append, boolean bufferedIO,
                        int bufferSize) throws IOException {
        super(layout, filename, append, bufferedIO, bufferSize);
    }

    public CustomizedFileAppender(Layout layout, String filename, boolean append) throws IOException {
        super(layout, filename, append);
    }

    public CustomizedFileAppender(Layout layout, String filename) throws IOException {
        super(layout, filename);
    }

    @Override
    public void activateOptions() {
        if(fileName != null) {
            try {
                final String CONN = "-yzq-";
                fileName = fileName.replace(".log", CONN +System.currentTimeMillis() + ".log");
                super.setFile(fileName, fileAppend, bufferedIO, bufferSize);
            }
            catch(java.io.IOException e) {
                errorHandler.error("setFile("+fileName+","+fileAppend+") call failed.",
                        e, ErrorCode.FILE_OPEN_FAILURE);
            }
        } else {
            LogLog.warn("File option not set for appender ["+name+"].");
            LogLog.warn("Are you using FileAppender instead of ConsoleAppender?");
        }
    }
}

package com.github.jwoschitz.avro.tool.utils;

import org.apache.avro.AvroTestUtil;

import java.io.File;
import java.io.IOException;

public class FileTestUtil {

    public static File createNewFile(Class testClass, String fileName) throws IOException {
        return createNewFile(testClass, fileName, null);
    }

    public static File createNewFile(Class testClass, String fileName, File parent) throws IOException {
        final File file = file(testClass, fileName, parent);
        return file.createNewFile() ? file : null;
    }

    public static File file(Class testClass, String fileName, File parent) {
        final File file = parent != null ?
                new File(parent, fileName) :
                AvroTestUtil.tempFile(testClass, fileName);

        file.deleteOnExit();

        return file;
    }

}

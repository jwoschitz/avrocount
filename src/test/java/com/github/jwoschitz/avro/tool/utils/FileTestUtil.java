package com.github.jwoschitz.avro.tool.utils;

import java.io.File;
import java.io.IOException;

public class FileTestUtil {
    static final File TMPDIR = new File(System.getProperty("test.dir", System.getProperty("java.io.tmpdir", "/tmp")), "tmpfiles");

    private static File tempFile(Class testClass, String name) {
        File testClassDir = new File(TMPDIR, testClass.getName());
        testClassDir.mkdirs();
        testClassDir.deleteOnExit();
        return new File(testClassDir, name);
    }

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
                tempFile(testClass, fileName);

        file.deleteOnExit();

        return file;
    }

}

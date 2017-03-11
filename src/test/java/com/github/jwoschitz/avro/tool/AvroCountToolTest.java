package com.github.jwoschitz.avro.tool;

import com.github.jwoschitz.avro.tool.utils.AvroDataFileGenerator;
import org.apache.avro.AvroTestUtil;
import org.apache.avro.file.CodecFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static com.github.jwoschitz.avro.tool.utils.AvroDataFileGenerator.intRecordGenerator;
import static org.junit.Assert.assertEquals;

public class AvroCountToolTest {

    @Rule
    public TestName testName = new TestName();

    @Test
    public void testCountOneFileNoCodec() throws Exception {
        File avroFile = intRecordGenerator(getClass(), CodecFactory.nullCodec())
                .createAvroFile(String.format("%s.avro", testName.getMethodName()), 1000);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            int returnCode = new AvroCountTool().run(
            System.in,
            new PrintStream(outputStream, true, StandardCharsets.UTF_8.toString()),
            System.err,
            Collections.singletonList(avroFile.getAbsolutePath())
        );

        assertEquals(0, returnCode);
        assertEquals("1000", new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim());
    }

    @Test
    public void testCountFilesInFolderNoCodec() throws Exception {
        AvroDataFileGenerator generator = intRecordGenerator(getClass(), CodecFactory.nullCodec());
        File folder = AvroTestUtil.tempDirectory(getClass(), testName.getMethodName());

        for (int i = 0; i < 10; i++) {
            generator.createAvroFile(String.format("%s_%s.avro", testName.getMethodName(), i), 1000, folder);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                new PrintStream(outputStream, true, StandardCharsets.UTF_8.toString()),
                System.err,
                Collections.singletonList(folder.getAbsolutePath())
        );

        assertEquals(0, returnCode);
        assertEquals("10000", new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim());
    }
}

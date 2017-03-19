package com.github.jwoschitz.avro.tool;

import org.apache.avro.file.CodecFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static com.github.jwoschitz.avro.tool.utils.AvroDataFileGenerator.intRecordGenerator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroCountToolCliTest {

    @Rule
    public TestName testName = new TestName();

    @Test
    public void testToolPrintsHelpIfNoPositionalArgsGiven() throws Exception {
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                System.out,
                new PrintStream(errorStream, true, StandardCharsets.UTF_8.toString()),
                Collections.emptyList()
        );

        String output = new String(errorStream.toByteArray(), StandardCharsets.UTF_8);

        assertEquals(1, returnCode);
        assertTrue(output.contains(new AvroCountTool().getShortDescription()));
    }

    @Test
    public void testToolPrintsHelpIfUnknownOptionIsGiven() throws Exception {
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                System.out,
                new PrintStream(errorStream, true, StandardCharsets.UTF_8.toString()),
                Arrays.asList("/some/path/to/avro/file.avro", "--doesNotExist")
        );

        String output = new String(errorStream.toByteArray(), StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(new StringReader(output));

        assertEquals(1, returnCode);
        assertTrue(output.contains(new AvroCountTool().getShortDescription()));
        assertEquals("'doesNotExist' is not a recognized option", reader.readLine());
    }

    @Test
    public void testMaxParallelismIsIgnoredIfNotNumeric() throws Exception {
        File avroFile = intRecordGenerator(getClass(), CodecFactory.nullCodec())
                .createAvroFile(String.format("%s.avro", testName.getMethodName()), 10);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                new PrintStream(outputStream, true, StandardCharsets.UTF_8.toString()),
                System.err,
                Arrays.asList(avroFile.getAbsolutePath(), "--maxParallelism=notNumeric")
        );

        assertEquals(0, returnCode);
        assertEquals("10", new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim());
    }
}

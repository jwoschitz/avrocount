package com.github.jwoschitz.avro.tool;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroCountToolCliTest {
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
}

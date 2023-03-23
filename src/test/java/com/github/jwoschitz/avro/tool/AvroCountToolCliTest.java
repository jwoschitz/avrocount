package com.github.jwoschitz.avro.tool;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

        List<String> results = reader.lines().collect(Collectors.toList());

        assertEquals(1, returnCode);
        assertTrue(output.contains(new AvroCountTool().getShortDescription()));
        assertEquals("doesNotExist is not a recognized option", results.get(0));
    }

    @Test
    public void testMaxParallelismIsNotAcceptedIfNotNumeric() throws Exception {
        ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                System.out,
                new PrintStream(errorStream, true, StandardCharsets.UTF_8.toString()),
                Arrays.asList("/some/path/to/avro/file.avro", "--maxParallelism=notNumeric")
        );

        String output = new String(errorStream.toByteArray(), StandardCharsets.UTF_8).trim();
        BufferedReader reader = new BufferedReader(new StringReader(output));

        List<String> results = reader.lines().collect(Collectors.toList());

        assertEquals(1, returnCode);
        assertEquals("Cannot parse argument 'notNumeric' of option maxParallelism", results.get(0));
    }
}

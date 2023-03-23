package com.github.jwoschitz.avro.tool;

import com.github.jwoschitz.avro.tool.utils.AvroDataFileGenerator;
import com.github.jwoschitz.avro.tool.utils.FileTestUtil;
import org.apache.avro.file.CodecFactory;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static com.github.jwoschitz.avro.tool.utils.AvroDataFileGenerator.intRecordGenerator;
import static org.junit.Assert.*;

public class AvroCountToolTest {

    @Rule
    public TestName testName = new TestName();

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testCountOneFileNoCodec() throws Exception {
        testCountOneFileWithCodec(null, 1000);
    }

    @Test
    public void testCountOneFileNullCodec() throws Exception {
        testCountOneFileWithCodec(CodecFactory.nullCodec(), 1000);
    }

    @Test
    public void testCountOneFileSnappy() throws Exception {
        testCountOneFileWithCodec(CodecFactory.snappyCodec(), 1000);
    }

    @Test
    public void testCountOneFileDeflate() throws Exception {
        testCountOneFileWithCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL), 1000);
    }

    @Test
    public void testCountOneFileBzip2() throws Exception {
        testCountOneFileWithCodec(CodecFactory.bzip2Codec(), 1000);
    }

    @Test
    public void testCountOneFileXz() throws Exception {
        testCountOneFileWithCodec(CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL), 1000);
    }

    @Test
    public void testCountSmallFileNullCodec() throws Exception {
        testCountOneFileWithCodec(CodecFactory.nullCodec(), 1);
    }

    @Test
    public void testCountSmallFileSnappyCodec() throws Exception {
        testCountOneFileWithCodec(CodecFactory.snappyCodec(), 1);
    }

    private void testCountOneFileWithCodec(CodecFactory codec, long recordCount) throws Exception {
        File avroFile = intRecordGenerator(getClass(), codec)
                .createAvroFile(String.format("%s.avro", testName.getMethodName()), recordCount);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                new PrintStream(outputStream, true, StandardCharsets.UTF_8.toString()),
                System.err,
                Collections.singletonList(avroFile.getAbsolutePath())
        );

        assertEquals(0, returnCode);
        assertEquals(String.valueOf(recordCount), new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim());
    }

    @Test
    public void testCountFilesInFolderNoCodec() throws Exception {
        testCountFilesInFolderWithCodec(null);
    }

    @Test
    public void testCountFilesInFolderNullCodec() throws Exception {
        testCountFilesInFolderWithCodec(CodecFactory.nullCodec());
    }

    @Test
    public void testCountFilesInFolderSnappy() throws Exception {
        testCountFilesInFolderWithCodec(CodecFactory.snappyCodec());
    }

    @Test
    public void testCountFilesInFolderDeflate() throws Exception {
        testCountFilesInFolderWithCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
    }

    @Test
    public void testCountFilesInFolderBzip2() throws Exception {
        testCountFilesInFolderWithCodec(CodecFactory.bzip2Codec());
    }

    @Test
    public void testCountFilesInFolderXz() throws Exception {
        testCountFilesInFolderWithCodec(CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL));
    }

    private void testCountFilesInFolderWithCodec(CodecFactory codec) throws Exception {
        AvroDataFileGenerator generator = intRecordGenerator(getClass(), codec);
        File folder = testFolder.newFolder(testName.getMethodName());

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

    @Test
    public void testIgnoreNonAvroSuffixedFile() throws Exception {
        File someFile = FileTestUtil.createNewFile(getClass(), "not_an_avro.file");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                new PrintStream(outputStream, true, StandardCharsets.UTF_8.toString()),
                System.err,
                Collections.singletonList(someFile.getAbsolutePath())
        );

        assertEquals(0, returnCode);
        assertEquals("0", new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim());
    }

    @Test
    public void testIgnoreNonAvroSuffixedFilesInFolder() throws Exception {
        AvroDataFileGenerator generator = intRecordGenerator(getClass(), CodecFactory.nullCodec());
        File folder = testFolder.newFolder(testName.getMethodName());

        for (int i = 0; i < 10; i++) {
            FileTestUtil.createNewFile(getClass(), String.format("not_an_avro_%s.file", i), folder);
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

    @Test
    public void testRaiseExceptionIfFileIsNotAvro() throws Exception {
        File someFile = FileTestUtil.createNewFile(getClass(), "not_an_avro.avro");

        try {
            new AvroCountTool().run(
                    System.in,
                    System.out,
                    System.err,
                    Collections.singletonList(someFile.getAbsolutePath())
            );
            fail("Should raise an exception if a '.avro' suffixed non-avro file is given");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Not a data file"));
        }
    }

    @Test
    @Ignore
    public void testBenchmark() throws Exception {
        AvroDataFileGenerator generator = intRecordGenerator(getClass(), CodecFactory.snappyCodec());
        File folder = testFolder.newFolder(testName.getMethodName());

        for (int i = 0; i < 100; i++) {
            generator.createAvroFile(String.format("%s_%s.avro", testName.getMethodName(), i), 10000000, folder);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                new PrintStream(outputStream, true, StandardCharsets.UTF_8.toString()),
                System.err,
                Collections.singletonList(folder.getAbsolutePath())
        );

        assertEquals(0, returnCode);
        assertEquals("1000000000", new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim());
    }

    @Test
    @Ignore
    public void testBenchmarkWithMinimalParallelism() throws Exception {
        AvroDataFileGenerator generator = intRecordGenerator(getClass(), CodecFactory.snappyCodec());
        File folder = testFolder.newFolder(testName.getMethodName());

        for (int i = 0; i < 100; i++) {
            generator.createAvroFile(String.format("%s_%s.avro", testName.getMethodName(), i), 10000000, folder);
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                new PrintStream(outputStream, true, StandardCharsets.UTF_8.toString()),
                System.err,
                Arrays.asList(folder.getAbsolutePath(), "--maxParallelism=1")
        );

        assertEquals(0, returnCode);
        assertEquals("1000000000", new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim());
    }

    @Test
    @Ignore
    public void testBenchmarkBigFile() throws Exception {
        AvroDataFileGenerator generator = intRecordGenerator(getClass(), CodecFactory.snappyCodec());
        File folder = testFolder.newFolder(testName.getMethodName());
        generator.createAvroFile(String.format("%s_%s.avro", testName.getMethodName(), 0), 100000000, folder);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int returnCode = new AvroCountTool().run(
                System.in,
                new PrintStream(outputStream, true, StandardCharsets.UTF_8.toString()),
                System.err,
                Collections.singletonList(folder.getAbsolutePath())
        );

        assertEquals(0, returnCode);
        assertEquals("100000000", new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim());
    }
}

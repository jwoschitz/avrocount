package com.github.jwoschitz.avro.tool;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

public class AvroCountToolTest {

    @Rule
    public TestName testName = new TestName();

    private class DataFileGenerator {
        private final Schema schema;
        private final CodecFactory codecFactory;
        private final BiFunction<Schema, Integer, GenericRecord> recordCreatorFn;

        DataFileGenerator(Schema schema, BiFunction<Schema, Integer, GenericRecord> recordCreatorFn, CodecFactory codecFactory) {
            this.schema = schema;
            this.codecFactory = codecFactory;
            this.recordCreatorFn = recordCreatorFn;
        }

        File createAvroFile(String fileName, int recordCount) throws Exception {
            return createAvroFile(fileName, recordCount, null);
        }

        File createAvroFile(String fileName, int recordCount, File parent) throws Exception {
            final File target = parent != null ?
                    new File(parent, fileName) :
                    AvroTestUtil.tempFile(getClass(), fileName);

            target.deleteOnExit();

            try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                if (codecFactory != null) {
                    writer.setCodec(codecFactory);
                }
                writer.create(schema, target);

                for (int i = 0; i < recordCount; i++) {
                    writer.append(recordCreatorFn.apply(schema, i));
                }
            }

            return target;
        }
    }

    private DataFileGenerator intRecordGenerator(CodecFactory codec) throws Exception {
        return new DataFileGenerator(
            new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("intRecord.avsc")),
            (schema, value) -> new GenericRecordBuilder(schema)
                    .set("value", value)
                    .build(),
            codec
        );
    }

    @Test
    public void testCountOneFileNoCodec() throws Exception {
        File avroFile = intRecordGenerator(CodecFactory.nullCodec())
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
        DataFileGenerator generator = intRecordGenerator(CodecFactory.nullCodec());
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

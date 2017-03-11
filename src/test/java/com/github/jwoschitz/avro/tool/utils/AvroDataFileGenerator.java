package com.github.jwoschitz.avro.tool.utils;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.File;
import java.util.function.BiFunction;

public class AvroDataFileGenerator {
    private final Schema schema;
    private final CodecFactory codecFactory;
    private final BiFunction<Schema, Integer, GenericRecord> recordCreatorFn;
    private final Class testClass;

    public AvroDataFileGenerator(Class testClass, Schema schema, BiFunction<Schema, Integer, GenericRecord> recordCreatorFn, CodecFactory codecFactory) {
        this.schema = schema;
        this.codecFactory = codecFactory;
        this.recordCreatorFn = recordCreatorFn;
        this.testClass = testClass;
    }

    public File createAvroFile(String fileName, int recordCount) throws Exception {
        return createAvroFile(fileName, recordCount, null);
    }

    public File createAvroFile(String fileName, int recordCount, File parent) throws Exception {
        final File target = parent != null ?
                new File(parent, fileName) :
                AvroTestUtil.tempFile(testClass, fileName);

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

    public static AvroDataFileGenerator intRecordGenerator(Class testClass, CodecFactory codec) throws Exception {
        return new AvroDataFileGenerator(
                testClass,
                new Schema.Parser().parse(testClass.getClassLoader().getResourceAsStream("intRecord.avsc")),
                (schema, value) -> new GenericRecordBuilder(schema)
                        .set("value", value)
                        .build(),
                codec
        );
    }
}

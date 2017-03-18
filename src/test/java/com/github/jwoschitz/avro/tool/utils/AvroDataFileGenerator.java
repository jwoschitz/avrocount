package com.github.jwoschitz.avro.tool.utils;

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
    private final BiFunction<Schema, Long, GenericRecord> recordCreatorFn;
    private final Class testClass;

    public AvroDataFileGenerator(Class testClass, Schema schema, BiFunction<Schema, Long, GenericRecord> recordCreatorFn, CodecFactory codecFactory) {
        this.schema = schema;
        this.codecFactory = codecFactory;
        this.recordCreatorFn = recordCreatorFn;
        this.testClass = testClass;
    }

    public File createAvroFile(String fileName, long recordCount) throws Exception {
        return createAvroFile(fileName, recordCount, null);
    }

    public File createAvroFile(String fileName, long recordCount, File parent) throws Exception {
        final File target = FileTestUtil.file(testClass, fileName, parent);

        try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            if (codecFactory != null) {
                writer.setCodec(codecFactory);
            }
            writer.create(schema, target);

            for (long i = 0; i < recordCount; i++) {
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

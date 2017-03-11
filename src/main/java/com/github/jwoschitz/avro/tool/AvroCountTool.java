package com.github.jwoschitz.avro.tool;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.tool.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;

public class AvroCountTool implements Tool {
    private final static Logger LOGGER = LoggerFactory.getLogger(AvroCountTool.class);

    private final static String SHORT_OPT_VERBOSE = "v";
    private final static String LONG_OPT_VERBOSE = "verbose";

    private static class BufferedAvroInputStream extends BufferedInputStream {
        private final String path;

        BufferedAvroInputStream(InputStream in, String path) {
            super(in);
            this.path = path;
        }

        String getPath() {
            return path;
        }
    }

    private static List<BufferedAvroInputStream> fileOrStdin(String path, final InputStream stdin) throws IOException {
        List<BufferedAvroInputStream> streams = new LinkedList<>();
        if (path.equals("-")) {
            LOGGER.debug("Using STDIN for input");
            streams.add(new BufferedAvroInputStream(stdin, path));
            return streams;
        }

        for(BufferedAvroInputStream handle : openFromFS(path)) {
            streams.add(handle);
        }

        return streams;
    }

    private static List<BufferedAvroInputStream> openFromFS(String path) throws IOException {
        List<BufferedAvroInputStream> streams = new LinkedList<>();
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());

        for (FileStatus status : fs.listStatus(p, filePath -> {
            boolean hasAvroSuffix = filePath.toString().endsWith(".avro");
            if (!hasAvroSuffix) {
                LOGGER.error("Ignoring file {}, does not have .avro suffix");
            }
            return hasAvroSuffix;
        })) {
            streams.add(new BufferedAvroInputStream(fs.open(status.getPath()), status.getPath().toString()));
        }

        return streams;
    }

    @Override
    public String getName() {
        return "count";
    }

    @Override
    public String getShortDescription() {
        return "Counts the records in an Avro data file";
    }

    @Override
    public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
        OptionParser optionParser = new OptionParser() {{
            acceptsAll(asList(SHORT_OPT_VERBOSE, LONG_OPT_VERBOSE), "Enable verbose mode");
            nonOptions("Path to an avro file or directory containing avro files, a dash ('-') can be given as an input file to use stdin")
                    .describedAs("pathToAvroFile")
                    .isRequired();
        }};

        List nargs = Collections.emptyList();
        try {
            OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));
            nargs = optionSet.nonOptionArguments();
        } catch (OptionException e) {
            err.println(e.getMessage());
        }

        if (nargs.size() < 1) {
            printHelp(err);
            err.println();
            optionParser.printHelpOn(err);
            return 1;
        }

        final long startedAt = System.currentTimeMillis();

        List<BufferedAvroInputStream> inStreams = fileOrStdin(nargs.get(0).toString(), stdin);

        long count = 0L;
        for (BufferedAvroInputStream inStream : inStreams) {
            GenericDatumReader<Object> reader = new GenericDatumReader<>();

            long startedProcessingAt = System.currentTimeMillis();
            LOGGER.debug("Processing {}", inStream.getPath());
            try (DataFileStream<Object> streamReader = new DataFileStream<>(inStream, reader)) {
                count += streamReader.getBlockCount();
                while (streamReader.hasNext()) {
                    streamReader.nextBlock();
                    count += streamReader.getBlockCount();
                }
            }
            LOGGER.debug("Processed {} in {}ms", inStream.getPath(), System.currentTimeMillis() - startedProcessingAt);
        }

        LOGGER.debug("Finished in {}ms", System.currentTimeMillis() - startedAt);

        err.flush();
        out.println(count);
        out.flush();

        return 0;
    }

    private void printHelp(PrintStream ps) {
        ps.println(getShortDescription());
    }

    public static void main(String[] args) throws Exception {
        boolean isVerbose = Arrays.stream(args)
                .map(x -> x.replace("-", ""))
                .anyMatch(x -> x.equalsIgnoreCase(SHORT_OPT_VERBOSE) || x.equalsIgnoreCase(LONG_OPT_VERBOSE));

        // rewrite console logger to stderr and set log level based on verbosity
        // keep logger implementation details out of AvroCountTool
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(isVerbose ? ch.qos.logback.classic.Level.DEBUG : ch.qos.logback.classic.Level.ERROR);

        ch.qos.logback.core.Appender<ch.qos.logback.classic.spi.ILoggingEvent> appender = root.getAppender("console");
        ((ch.qos.logback.core.ConsoleAppender) appender).setTarget(ch.qos.logback.core.joran.spi.ConsoleTarget.SystemErr.getName());
        appender.start();

        int rc = (new AvroCountTool()).run(System.in, System.out, System.err, asList(args));
        System.exit(rc);
    }
}

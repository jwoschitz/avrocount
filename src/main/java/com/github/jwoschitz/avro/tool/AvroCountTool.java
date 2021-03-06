package com.github.jwoschitz.avro.tool;

import com.github.jwoschitz.avro.file.CountableSkipDataFileStream;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
                LOGGER.error("Ignoring file {}, does not have .avro suffix", filePath.toString());
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
            accepts("maxParallelism", "Maximum amount of parallelism")
                    .withRequiredArg()
                    .defaultsTo("-1")
                    .ofType(Integer.class);
            nonOptions("Path to an avro file or directory containing avro files, a dash ('-') can be given as an input file to use stdin")
                    .describedAs("pathToAvroFile")
                    .isRequired();
        }};

        List nargs = Collections.emptyList();

        int maxParallelism = -1;
        try {
            OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));
            maxParallelism = Integer.parseInt(optionSet.valueOf("maxParallelism").toString());
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

        long totalCount = 0L;

        if (inStreams.size() > 0) {
            int threadCount = maxParallelism > 0 ? Math.min(maxParallelism, inStreams.size()) : inStreams.size();
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            List<Future<Long>> processors = new LinkedList<>();

            for (final BufferedAvroInputStream inStream : inStreams) {
                Future<Long> processor = executor.submit(() -> {
                    try {
                        LOGGER.debug("Started to process {}", inStream.getPath());

                        long count = 0L;

                        long startedProcessingAt = System.currentTimeMillis();
                        try (CountableSkipDataFileStream streamReader = new CountableSkipDataFileStream(inStream)) {
                            while (streamReader.hasNextBlock()) {
                                streamReader.nextBlock();
                                count += streamReader.getBlockCount();
                            }
                        }

                        LOGGER.debug("Processed {} in {}ms", inStream.getPath(), System.currentTimeMillis() - startedProcessingAt);
                        return count;
                    } catch (Exception e) {
                        LOGGER.error(String.format("Error occurred while processing %s", inStream.getPath()), e);
                        throw e;
                    }
                });
                processors.add(processor);
            }

            for (Future<Long> processor : processors) {
                totalCount += processor.get();
            }
        }

        LOGGER.debug("Finished in {}ms", System.currentTimeMillis() - startedAt);

        err.flush();
        out.println(totalCount);
        out.flush();

        return 0;
    }

    private void printHelp(PrintStream ps) {
        ps.println(getShortDescription());
    }

    private static boolean redirectLogger(Logger logger, boolean isVerbose) {
        if (logger instanceof ch.qos.logback.classic.Logger) {
            try {
                // rewrite console logger to stderr and set log level based on verbosity
                // keep logger implementation details out of AvroCountTool
                ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
                root.setLevel(isVerbose ? ch.qos.logback.classic.Level.DEBUG : ch.qos.logback.classic.Level.ERROR);

                ch.qos.logback.core.Appender<ch.qos.logback.classic.spi.ILoggingEvent> appender = root.getAppender("console");
                ((ch.qos.logback.core.ConsoleAppender) appender).setTarget(ch.qos.logback.core.joran.spi.ConsoleTarget.SystemErr.getName());
                appender.start();

                return true;
            } catch (Exception e) {
                LOGGER.warn("An unexpected error occurred while trying to redirect logger", e);
            }
        }
        return false;
    }

    public static void main(String[] args) throws Exception {
        boolean isVerbose = Arrays.stream(args)
                .map(x -> x.replace("-", ""))
                .anyMatch(x -> x.equalsIgnoreCase(SHORT_OPT_VERBOSE) || x.equalsIgnoreCase(LONG_OPT_VERBOSE));

        boolean isRedirected = redirectLogger(LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME), isVerbose);
        if (!isRedirected && isVerbose) {
            LOGGER.warn("Unable to redirect logger, verbose output might potentially not work");
        }

        int rc = (new AvroCountTool()).run(System.in, System.out, System.err, asList(args));
        System.exit(rc);
    }
}

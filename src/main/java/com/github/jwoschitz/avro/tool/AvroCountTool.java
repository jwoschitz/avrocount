package com.github.jwoschitz.avro.tool;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.tool.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class AvroCountTool implements Tool {

    private static List<BufferedInputStream> fileOrStdin(String path, final InputStream stdin) throws IOException {
        List<BufferedInputStream> streams = new LinkedList<>();
        if (path.equals("-")) {
            streams.add(new BufferedInputStream(stdin));
            return streams;
        }

        for(InputStream handle : openFromFS(path)) {
            streams.add(new BufferedInputStream(handle));
        }

        return streams;
    }

    private static List<InputStream> openFromFS(String path) throws IOException {
        List<InputStream> streams = new LinkedList<>();
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        if (fs.isFile(p)) {
            streams.add(fs.open(p));
            return streams;
        }

        for (FileStatus status : fs.listStatus(p, filePath -> filePath.toString().endsWith(".avro"))) {
            streams.add(fs.open(status.getPath()));
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
        OptionParser optionParser = new OptionParser();

        OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));
        List<String> nargs = (List<String>)optionSet.nonOptionArguments();

        if (nargs.size() != 1) {
            printHelp(err);
            err.println();
            optionParser.printHelpOn(err);
            return 1;
        }

        List<BufferedInputStream> inStreams = fileOrStdin(nargs.get(0), stdin);

        long count = 0L;
        for (BufferedInputStream inStream : inStreams) {
            GenericDatumReader<Object> reader = new GenericDatumReader<Object>();

            try (DataFileStream<Object> streamReader = new DataFileStream<Object>(inStream, reader)) {
                count += streamReader.getBlockCount();
                while (streamReader.hasNext()) {
                    streamReader.nextBlock();
                    count += streamReader.getBlockCount();
                }
            }
        }

        err.flush();
        out.println(count);
        out.flush();

        return 0;
    }

    private void printHelp(PrintStream ps) {
        ps.println(getShortDescription());
        ps.println("A dash ('-') can be given as an input file to use stdin");
    }

    public static void main(String[] args) throws Exception {
        int rc = (new AvroCountTool()).run(System.in, System.out, System.err, Arrays.asList(args));
        System.exit(rc);
    }
}

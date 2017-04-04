package com.github.jwoschitz.avro.file;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Optimized version of org.apache.avro.file.DataFileStream which is only reading the data
 * from the InputStream which is necessary to retrieve the block count
 *
 * The focus of this implementation is counting and it is not providing features for record deserialization.
 * For everything apart counting avro files, use the original DataFileStream implementation.
 *
 * @see org.apache.avro.file.DataFileStream
 */
public class CountableSkipDataFileStream implements Closeable {

    private BinaryDecoder vin;

    private boolean availableBlock = false;
    private long blockSize;
    private long blockCount;

    private byte[] expectedSync = new byte[DataFileConstants.SYNC_SIZE];
    private byte[] syncBuffer = new byte[DataFileConstants.SYNC_SIZE];

    public CountableSkipDataFileStream(InputStream in) throws IOException {
        initialize(in);
    }

    private void initialize(InputStream in) throws IOException {
        this.vin = DecoderFactory.get().binaryDecoder(in, vin);
        byte[] magic = new byte[DataFileConstants.MAGIC.length];
        try {
            vin.readFixed(magic);
        } catch (IOException e) {
            throw new IOException("Not a data file.", e);
        }
        if (!Arrays.equals(DataFileConstants.MAGIC, magic))
            throw new IOException("Not a data file.");

        long l = vin.readMapStart();
        if (l > 0) {
            do {
                for (long i = 0; i < l; i++) {
                    vin.skipString();
                    vin.skipBytes();
                }
            } while ((l = vin.mapNext()) != 0);
        }
        vin.readFixed(expectedSync);
    }

    public long getBlockCount() { return blockCount; }

    public boolean hasNextBlock() {
        try {
            if (availableBlock) return true;
            if (vin.isEnd()) return false;
            final long blockRemaining = vin.readLong();
            blockSize = vin.readLong();
            if (blockSize > Integer.MAX_VALUE ||
                    blockSize < 0) {
                throw new IOException("Block size invalid or too large for this " +
                        "implementation: " + blockSize);
            }
            blockCount = blockRemaining;
            availableBlock = true;
            return true;
        } catch (EOFException eof) {
            return false;
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
    }

    public void nextBlock() throws IOException {
        if (!hasNextBlock()) {
            throw new NoSuchElementException();
        }
        vin.skipFixed((int) blockSize);
        vin.readFixed(syncBuffer);
        availableBlock = false;
        if (!Arrays.equals(syncBuffer, expectedSync))
            throw new IOException("Invalid sync!");
    }

    @Override
    public void close() throws IOException {
        vin.inputStream().close();
    }
}

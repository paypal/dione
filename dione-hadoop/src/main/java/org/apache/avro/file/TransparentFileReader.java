package org.apache.avro.file;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Used to expose internal properties
 */
public class TransparentFileReader extends DataFileReader<GenericRecord> {
    public TransparentFileReader(SeekableInput in, DatumReader<GenericRecord> reader) throws IOException {
        super(in, reader);
    }

    public ByteBuffer getCurrentBlock() {
        return blockBuffer;
    }
}

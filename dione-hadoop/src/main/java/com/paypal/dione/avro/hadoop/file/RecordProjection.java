package com.paypal.dione.avro.hadoop.file;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import static com.paypal.dione.avro.hadoop.file.AvroBtreeFile.METADATA_COL_NAME;

public class RecordProjection {
    private final Schema keySchema;
    private final Schema valueSchema;


    public RecordProjection(Schema keySchema, Schema valueSchema) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public GenericRecord getKey(GenericRecord record) {
        return projectSchema(record, keySchema);
    }

    public GenericRecord getValue(GenericRecord record) {
        Schema schema = this.valueSchema;
        return projectSchema(record, schema);
    }

    public Long getMetadata(GenericRecord record) {
        GenericData.Record res = new GenericData.Record(keySchema);
        return (Long) record.get(METADATA_COL_NAME);
    }

    private GenericRecord projectSchema(GenericRecord record, Schema schema) {
        GenericData.Record res = new GenericData.Record(schema);
        schema.getFields().stream().map(Schema.Field::name).forEach(f -> {
            res.put(f, record.get(f));
        });
        return res;
    }
}

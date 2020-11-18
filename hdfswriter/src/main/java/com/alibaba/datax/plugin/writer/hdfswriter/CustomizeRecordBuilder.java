package com.alibaba.datax.plugin.writer.hdfswriter;


import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.data.RecordBuilderBase;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;

import java.io.IOException;

public class CustomizeRecordBuilder extends RecordBuilderBase<Record> {
    private final Record record;

    public CustomizeRecordBuilder(Schema schema) {
        super(schema, GenericData.get());
        this.record = new Record(schema);
    }

    public CustomizeRecordBuilder set(String fieldName, Object value) {
        Schema.Field field = this.schema().getField(fieldName);
        int pos = field.pos();
        this.record.put(field.pos(), value);
        this.fieldSetFlags()[pos] = true;
        return this;
    }

    @Override
    public Record build() {
        Record record;
        try {
            record = new Record(this.schema());
        } catch (Exception var9) {
            throw new AvroRuntimeException(var9);
        }

        Schema.Field[] var2 = this.fields();
        int var3 = var2.length;

        for (int var4 = 0; var4 < var3; ++var4) {
            Schema.Field field = var2[var4];

            Object value;
            try {
                value = this.getWithDefault(field);
            } catch (IOException var8) {
                throw new AvroRuntimeException(var8);
            }

            if (value != null) {
                record.put(field.pos(), value);
            }
        }

        return record;
    }

    private Object getWithDefault(Schema.Field field) throws IOException {
        return this.fieldSetFlags()[field.pos()] ? this.record.get(field.pos()) : this.defaultValue(field);
    }

    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (this.record == null ? 0 : this.record.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!super.equals(obj)) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            CustomizeRecordBuilder other = (CustomizeRecordBuilder)obj;
            if (this.record == null) {
                return other.record == null;
            } else {
                return this.record.equals(other.record);
            }
        }
    }
}

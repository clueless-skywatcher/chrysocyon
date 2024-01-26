package com.github.cluelessskywatcher.chrysocyon.tuples;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.github.cluelessskywatcher.chrysocyon.tuples.TupleLayout;
import com.github.cluelessskywatcher.chrysocyon.tuples.TupleSchema;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.IntegerInfo;
import com.github.cluelessskywatcher.chrysocyon.tuples.info.VarStringInfo;

public class TupleSchemaTest {
    @Test
    public void testSchema() {
        TupleSchema schema = new TupleSchema();
        schema.addField(new IntegerInfo(), "field1");
        schema.addField(new VarStringInfo(20), "field2");
        schema.addField(new IntegerInfo(), "field3");

        TupleLayout layout = new TupleLayout(schema);

        Map<String, Integer> offsets = Map.of(
            "field1", 4,
            "field2", 8,
            "field3", 32
        );

        Assertions.assertEquals(offsets, layout.getOffsets());
        Assertions.assertEquals(36, layout.getSlotSize());
    }
}

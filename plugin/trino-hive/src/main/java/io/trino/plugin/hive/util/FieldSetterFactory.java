/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.util;

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class FieldSetterFactory
{
    public FieldSetter create(SettableStructObjectInspector rowInspector, Object row, StructField field, Type type)
    {
        requireNonNull(rowInspector, "row inspector is null");
        requireNonNull(row, "row is null");
        requireNonNull(field, "field is null");
        FieldTranslator<?> fieldTranslator = getFieldTranslator(type).get();
        return (block, position) ->
                rowInspector.setStructFieldData(row, field, fieldTranslator.getHiveValue(block, position));
    }

    protected Supplier<FieldTranslator<?>> getFieldTranslator(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return getTranslator(
                    BooleanWritable::new,
                    (state, block, position) -> state.set(BOOLEAN.getBoolean(block, position)));
        }
        if (BIGINT.equals(type)) {
            return getTranslator(
                    LongWritable::new,
                    (state, block, position) -> state.set(BIGINT.getLong(block, position)));
        }
        if (INTEGER.equals(type)) {
            return getTranslator(
                    IntWritable::new,
                    (state, block, position) -> state.set(toIntExact(INTEGER.getLong(block, position))));
        }
        if (SMALLINT.equals(type)) {
            return getTranslator(
                    ShortWritable::new,
                    (state, block, position) -> state.set(Shorts.checkedCast(SMALLINT.getLong(block, position))));
        }
        if (TINYINT.equals(type)) {
            return getTranslator(
                    ByteWritable::new,
                    (state, block, position) -> state.set(SignedBytes.checkedCast(TINYINT.getLong(block, position))));
        }
        if (REAL.equals(type)) {
            return getTranslator(
                    FloatWritable::new,
                    (state, block, position) -> state.set(intBitsToFloat((int) REAL.getLong(block, position))));
        }
        if (DOUBLE.equals(type)) {
            return getTranslator(
                    DoubleWritable::new,
                    (state, block, position) -> state.set(DOUBLE.getDouble(block, position)));
        }
        if (type instanceof VarcharType) {
            return getTranslator(
                    Text::new,
                    (state, block, position) -> state.set(type.getSlice(block, position).getBytes()));
        }
        if (type instanceof CharType) {
            return getTranslator(
                    Text::new,
                    (state, block, position) -> state.set(type.getSlice(block, position).getBytes()));
        }
        if (VARBINARY.equals(type)) {
            return getTranslator(
                    BytesWritable::new,
                    (state, block, position) -> {
                        byte[] bytes = VARBINARY.getSlice(block, position).getBytes();
                        state.set(bytes, 0, bytes.length);
                    });
        }
        if (DATE.equals(type)) {
            return getTranslator(
                    DateWritableV2::new,
                    (state, block, position) -> state.set(toIntExact(DATE.getLong(block, position))));
        }
        if (type instanceof TimestampType) {
            return getTranslator(
                    TimestampWritableV2::new,
                    (state, block, position) -> state.set(getHiveTimestamp((TimestampType) type, block, position)));
        }
        if (type instanceof DecimalType) {
            return getTranslator(
                    HiveDecimalWritable::new,
                    (state, block, position) -> state.set(getHiveDecimal((DecimalType) type, block, position)));
        }
        if (type instanceof ArrayType) {
            return () -> new ArrayFieldTranslator((ArrayType) type);
        }
        if (type instanceof MapType) {
            return () -> new MapFieldTranslator((MapType) type);
        }
        if (type instanceof RowType) {
            return () -> new RowFieldTranslator((RowType) type);
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    protected static <V> Supplier<FieldTranslator<?>> getTranslator(Supplier<V> stateSupplier, StateSetter<V> setter)
    {
        return () -> new StatefulFieldTranslator<>(stateSupplier.get())
        {
            @Override
            protected void setValue(Block block, int position)
            {
                setter.setValue(state, block, position);
            }
        };
    }

    /**
     * An object that represents the action of setting a field of a Hive-compatible object.
     */
    @FunctionalInterface
    public interface FieldSetter
    {
        void setField(Block block, int position);
    }

    /**
     * Similar to a {@link FieldSetter}, but with caller-provided state.
     */
    @FunctionalInterface
    private interface StateSetter<V>
    {
        void setValue(V state, Block block, int position);
    }

    protected interface FieldTranslator<V>
    {
        /**
         * Get a Hive object representing a value from the block.
         *
         * <p>This is not guaranteed to return a new object for each call, and
         * in many cases it will not (see {@link StatefulFieldTranslator}).
         */
        V getHiveValue(Block block, int position);
    }

    /**
     * A {@link FieldTranslator} with a stateful Hive value.
     *
     * <p>Reuses and returns the same object for each call to {@link #getHiveValue(Block, int)}.
     */
    protected abstract static class StatefulFieldTranslator<V>
            implements FieldTranslator<V>
    {
        protected final V state;

        protected StatefulFieldTranslator(V state)
        {
            this.state = requireNonNull(state, "state is null");
        }

        /**
         * Write from the specified block and position to this instance's {@link #state}.
         */
        protected abstract void setValue(Block block, int position);

        @Override
        public final V getHiveValue(Block block, int position)
        {
            setValue(block, position);
            return state;
        }
    }

    private class ArrayFieldTranslator
            implements FieldTranslator<List<Object>>
    {
        private final Supplier<FieldTranslator<?>> elementTranslator;

        ArrayFieldTranslator(ArrayType type)
        {
            elementTranslator = getFieldTranslator(type.getElementType());
        }

        @Override
        public List<Object> getHiveValue(Block block, int position)
        {
            Block arrayBlock = block.getObject(position, Block.class);
            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                list.add(elementTranslator.get().getHiveValue(arrayBlock, i));
            }
            return list;
        }
    }

    private class MapFieldTranslator
            implements FieldTranslator<Map<Object, Object>>
    {
        private final Supplier<FieldTranslator<?>> keyTranslator;
        private final Supplier<FieldTranslator<?>> valueTranslator;

        MapFieldTranslator(MapType type)
        {
            keyTranslator = getFieldTranslator(type.getKeyType());
            valueTranslator = getFieldTranslator(type.getValueType());
        }

        @Override
        public Map<Object, Object> getHiveValue(Block block, int position)
        {
            Block mapBlock = block.getObject(position, Block.class);
            Map<Object, Object> map = new HashMap<>(mapBlock.getPositionCount() * 2);
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                map.put(
                        keyTranslator.get().getHiveValue(mapBlock, i),
                        valueTranslator.get().getHiveValue(mapBlock, i + 1));
            }
            return map;
        }
    }

    private class RowFieldTranslator
            implements FieldTranslator<List<Object>>
    {
        List<Supplier<FieldTranslator<?>>> fieldTranslators;

        RowFieldTranslator(RowType type)
        {
            fieldTranslators = type.getTypeParameters().stream()
                    .map(FieldSetterFactory.this::getFieldTranslator)
                    .collect(toUnmodifiableList());
        }

        @Override
        public List<Object> getHiveValue(Block block, int position)
        {
            Block rowBlock = block.getObject(position, Block.class);
            List<Object> value = new ArrayList<>(fieldTranslators.size());
            for (int i = 0; i < fieldTranslators.size(); i++) {
                value.add(fieldTranslators.get(i).get().getHiveValue(rowBlock, i));
            }
            return value;
        }
    }

    private static HiveDecimal getHiveDecimal(DecimalType decimalType, Block block, int position)
    {
        BigInteger unscaledValue;
        if (decimalType.isShort()) {
            unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
        }
        else {
            unscaledValue = Decimals.decodeUnscaledValue(decimalType.getSlice(block, position));
        }
        return HiveDecimal.create(unscaledValue, decimalType.getScale());
    }

    private static Timestamp getHiveTimestamp(TimestampType type, Block block, int position)
    {
        verify(type.getPrecision() <= HiveTimestampPrecision.MAX.getPrecision(), "Timestamp precision too high for Hive");

        long epochMicro;
        int nanoOfMicro;
        if (type.isShort()) {
            epochMicro = type.getLong(block, position);
            nanoOfMicro = 0;
        }
        else {
            LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
            epochMicro = timestamp.getEpochMicros();
            nanoOfMicro = timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;
        }
        long epochSecond = floorDiv(epochMicro, MICROSECONDS_PER_SECOND);
        int microOfSecond = floorMod(epochMicro, MICROSECONDS_PER_SECOND);
        int nanoOfSecond = microOfSecond * NANOSECONDS_PER_MICROSECOND + nanoOfMicro;
        return Timestamp.ofEpochSecond(epochSecond, nanoOfSecond);
    }
}

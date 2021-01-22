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
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
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
import org.apache.hadoop.hive.common.type.Date;
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

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
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
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class FieldSetterFactory
{
    public FieldSetter create(SettableStructObjectInspector rowInspector, Object row, StructField field, Type type)
    {
        return new TranslatingFieldSetter(rowInspector, row, field, getFieldTranslator(type));
    }

    protected FieldTranslator<?> getFieldTranslator(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return new BooleanFieldTranslator();
        }
        if (BIGINT.equals(type)) {
            return new BigintFieldTranslator();
        }
        if (INTEGER.equals(type)) {
            return new IntFieldTranslator();
        }
        if (SMALLINT.equals(type)) {
            return new SmallintFieldTranslator();
        }
        if (TINYINT.equals(type)) {
            return new TinyintFieldTranslator();
        }
        if (REAL.equals(type)) {
            return new FloatFieldTranslator();
        }
        if (DOUBLE.equals(type)) {
            return new DoubleFieldTranslator();
        }
        if (type instanceof VarcharType) {
            return new VarcharFieldTranslator(type);
        }
        if (type instanceof CharType) {
            return new CharFieldTranslator(type);
        }
        if (VARBINARY.equals(type)) {
            return new BinaryFieldTranslator();
        }
        if (DATE.equals(type)) {
            return new DateFieldTranslator();
        }
        if (type instanceof TimestampType) {
            return new TimestampFieldTranslator((TimestampType) type);
        }
        if (type instanceof DecimalType) {
            return new DecimalFieldTranslator((DecimalType) type);
        }
        if (type instanceof ArrayType) {
            return new ArrayFieldTranslator((ArrayType) type);
        }
        if (type instanceof MapType) {
            return new MapFieldTranslator((MapType) type);
        }
        if (type instanceof RowType) {
            return new RowFieldTranslator((RowType) type);
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public interface FieldSetter
    {
        void setField(Block block, int position);
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
     * Basic implementation of {@link FieldSetter} that handles the actual
     * field-setting, leaving the data translation to a {@link FieldTranslator}.
     */
    protected static class TranslatingFieldSetter
            implements FieldSetter
    {
        private final SettableStructObjectInspector inspector;
        private final Object row;
        private final StructField field;
        private final FieldTranslator<?> fieldTranslator;

        protected TranslatingFieldSetter(SettableStructObjectInspector inspector, Object row, StructField field, FieldTranslator<?> fieldTranslator)
        {
            this.inspector = requireNonNull(inspector, "inspector is null");
            this.row = requireNonNull(row, "row is null");
            this.field = requireNonNull(field, "field is null");
            this.fieldTranslator = requireNonNull(fieldTranslator, "field mapper is null");
        }

        @Override
        public final void setField(Block block, int position)
        {
            inspector.setStructFieldData(row, field, fieldTranslator.getHiveValue(block, position));
        }
    }

    /**
     * An {@link FieldTranslator} with a stateful Hive value.
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

    private static class BooleanFieldTranslator
            extends StatefulFieldTranslator<BooleanWritable>
    {
        BooleanFieldTranslator()
        {
            super(new BooleanWritable());
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(BOOLEAN.getBoolean(block, position));
        }
    }

    private static class BigintFieldTranslator
            extends StatefulFieldTranslator<LongWritable>
    {
        BigintFieldTranslator()
        {
            super(new LongWritable());
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(BIGINT.getLong(block, position));
        }
    }

    private static class IntFieldTranslator
            extends StatefulFieldTranslator<IntWritable>
    {
        IntFieldTranslator()
        {
            super(new IntWritable());
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(toIntExact(INTEGER.getLong(block, position)));
        }
    }

    private static class SmallintFieldTranslator
            extends StatefulFieldTranslator<ShortWritable>
    {
        SmallintFieldTranslator()
        {
            super(new ShortWritable());
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(Shorts.checkedCast(SMALLINT.getLong(block, position)));
        }
    }

    private static class TinyintFieldTranslator
            extends StatefulFieldTranslator<ByteWritable>
    {
        TinyintFieldTranslator()
        {
            super(new ByteWritable());
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(SignedBytes.checkedCast(TINYINT.getLong(block, position)));
        }
    }

    private static class DoubleFieldTranslator
            extends StatefulFieldTranslator<DoubleWritable>
    {
        DoubleFieldTranslator()
        {
            super(new DoubleWritable());
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(DOUBLE.getDouble(block, position));
        }
    }

    private static class FloatFieldTranslator
            extends StatefulFieldTranslator<FloatWritable>
    {
        FloatFieldTranslator()
        {
            super(new FloatWritable());
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(intBitsToFloat((int) REAL.getLong(block, position)));
        }
    }

    private static class VarcharFieldTranslator
            extends StatefulFieldTranslator<Text>
    {
        private final Type type;

        VarcharFieldTranslator(Type type)
        {
            super(new Text());
            this.type = type;
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(type.getSlice(block, position).getBytes());
        }
    }

    private static class CharFieldTranslator
            extends StatefulFieldTranslator<Text>
    {
        private final Type type;

        CharFieldTranslator(Type type)
        {
            super(new Text());
            this.type = type;
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(type.getSlice(block, position).getBytes());
        }
    }

    private static class BinaryFieldTranslator
            extends StatefulFieldTranslator<BytesWritable>
    {
        BinaryFieldTranslator()
        {
            super(new BytesWritable());
        }

        @Override
        public void setValue(Block block, int position)
        {
            byte[] bytes = VARBINARY.getSlice(block, position).getBytes();
            state.set(bytes, 0, bytes.length);
        }
    }

    private static class DateFieldTranslator
            extends StatefulFieldTranslator<DateWritableV2>
    {
        DateFieldTranslator()
        {
            super(new DateWritableV2());
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(toIntExact(DATE.getLong(block, position)));
        }
    }

    private static class TimestampFieldTranslator
            extends StatefulFieldTranslator<TimestampWritableV2>
    {
        private final TimestampType type;

        TimestampFieldTranslator(TimestampType type)
        {
            super(new TimestampWritableV2());
            this.type = requireNonNull(type, "type is null");
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(getHiveTimestamp(type, block, position));
        }
    }

    private static class DecimalFieldTranslator
            extends StatefulFieldTranslator<HiveDecimalWritable>
    {
        private final DecimalType decimalType;

        DecimalFieldTranslator(DecimalType decimalType)
        {
            super(new HiveDecimalWritable());
            this.decimalType = decimalType;
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(getHiveDecimal(decimalType, block, position));
        }
    }

    private class ArrayFieldTranslator
            implements FieldTranslator<List<Object>>
    {
        private final ArrayType type;

        ArrayFieldTranslator(ArrayType type)
        {
            this.type = requireNonNull(type, "array type is null");
        }

        @Override
        public List<Object> getHiveValue(Block block, int position)
        {
            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                list.add(getField(type.getElementType(), arrayBlock, i));
            }
            return list;
        }
    }

    private class MapFieldTranslator
            implements FieldTranslator<Map<Object, Object>>
    {
        private final MapType type;

        MapFieldTranslator(MapType type)
        {
            this.type = requireNonNull(type, "map type is null");
        }

        @Override
        public Map<Object, Object> getHiveValue(Block block, int position)
        {
            Block mapBlock = block.getObject(position, Block.class);

            Map<Object, Object> map = new HashMap<>(mapBlock.getPositionCount() * 2);
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                map.put(
                        getField(type.getKeyType(), mapBlock, i),
                        getField(type.getValueType(), mapBlock, i + 1));
            }
            return map;
        }
    }

    private class RowFieldTranslator
            implements FieldTranslator<List<Object>>
    {
        private final RowType type;

        RowFieldTranslator(RowType type)
        {
            this.type = requireNonNull(type, "row type is null");
        }

        @Override
        public List<Object> getHiveValue(Block block, int position)
        {
            Block rowBlock = block.getObject(position, Block.class);

            // TODO reuse row object and use FieldSetters, like we do at the top level
            // Ideally, we'd use the same recursive structure starting from the top, but
            // this requires modeling row types in the same way we model table rows
            // (multiple blocks vs all fields packed in a single block)
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> value = new ArrayList<>(fieldTypes.size());
            for (int i = 0; i < fieldTypes.size(); i++) {
                value.add(getField(fieldTypes.get(i), rowBlock, i));
            }
            return value;
        }
    }

    protected Object getField(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        if (BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        if (INTEGER.equals(type)) {
            return toIntExact(type.getLong(block, position));
        }
        if (SMALLINT.equals(type)) {
            return Shorts.checkedCast(type.getLong(block, position));
        }
        if (TINYINT.equals(type)) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }
        if (REAL.equals(type)) {
            return intBitsToFloat((int) type.getLong(block, position));
        }
        if (DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return new Text(type.getSlice(block, position).getBytes());
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            return new Text(padSpaces(type.getSlice(block, position), charType).toStringUtf8());
        }
        if (VARBINARY.equals(type)) {
            return type.getSlice(block, position).getBytes();
        }
        if (DATE.equals(type)) {
            return Date.ofEpochDay(toIntExact(type.getLong(block, position)));
        }
        if (type instanceof TimestampType) {
            return getHiveTimestamp((TimestampType) type, block, position);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getHiveDecimal(decimalType, block, position);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                list.add(getField(elementType, arrayBlock, i));
            }
            return unmodifiableList(list);
        }
        if (type instanceof MapType) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            Block mapBlock = block.getObject(position, Block.class);

            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                map.put(
                        getField(keyType, mapBlock, i),
                        getField(valueType, mapBlock, i + 1));
            }
            return unmodifiableMap(map);
        }
        if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();
            Block rowBlock = block.getObject(position, Block.class);
            checkCondition(
                    fieldTypes.size() == rowBlock.getPositionCount(),
                    StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Expected row value field count does not match type field count");
            List<Object> row = new ArrayList<>(rowBlock.getPositionCount());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                row.add(getField(fieldTypes.get(i), rowBlock, i));
            }
            return unmodifiableList(row);
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
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

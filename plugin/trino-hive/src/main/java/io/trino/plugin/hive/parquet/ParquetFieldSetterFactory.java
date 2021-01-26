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
package io.trino.plugin.hive.parquet;

import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.util.FieldSetterFactory;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.joda.time.DateTimeZone;

import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Objects.requireNonNull;

public class ParquetFieldSetterFactory
        extends FieldSetterFactory
{
    private final DateTimeZone timeZone;

    public ParquetFieldSetterFactory(DateTimeZone timeZone)
    {
        this.timeZone = requireNonNull(timeZone, "time zone is null");
    }

    @Override
    protected Supplier<FieldTranslator<?>> getFieldTranslator(Type type)
    {
        if (type instanceof TimestampType) {
            return () -> new TimestampFieldTranslator((TimestampType) type);
        }
        return super.getFieldTranslator(type);
    }

    private class TimestampFieldTranslator
            extends StatefulFieldTranslator<TimestampWritableV2>
    {
        private final TimestampType type;

        public TimestampFieldTranslator(TimestampType type)
        {
            super(new TimestampWritableV2());
            this.type = requireNonNull(type, "type is null");

            verify(type.getPrecision() <= HiveTimestampPrecision.MAX.getPrecision(), "Timestamp precision too high for Hive");
        }

        @Override
        public void setValue(Block block, int position)
        {
            state.set(getHiveTimestamp(type, block, position));
        }
    }

    private Timestamp getHiveTimestamp(TimestampType type, Block block, int position)
    {
        long localEpochMicro;
        int nanoOfMicro;
        if (type.isShort()) {
            localEpochMicro = type.getLong(block, position);
            nanoOfMicro = 0;
        }
        else {
            LongTimestamp longTimestamp = (LongTimestamp) type.getObject(block, position);
            localEpochMicro = longTimestamp.getEpochMicros();
            nanoOfMicro = longTimestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND;
        }
        int microOfSecond = floorMod(localEpochMicro, MICROSECONDS_PER_SECOND);
        int nanoOfSecond = microOfSecond * NANOSECONDS_PER_MICROSECOND + nanoOfMicro;

        long localEpochMilli = floorDiv(localEpochMicro, MICROSECONDS_PER_MILLISECOND);
        long utcEpochMilli = timeZone.convertLocalToUTC(localEpochMilli, false);
        long utcEpochSecond = floorDiv(utcEpochMilli, MILLISECONDS_PER_SECOND);

        return Timestamp.ofEpochSecond(utcEpochSecond, nanoOfSecond);
    }
}

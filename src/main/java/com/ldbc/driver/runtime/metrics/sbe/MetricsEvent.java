/* Generated SBE (Simple Binary Encoding) message codec */
package com.ldbc.driver.runtime.metrics.sbe;

import uk.co.real_logic.sbe.codec.java.*;

public class MetricsEvent
{
    public static final int BLOCK_LENGTH = 41;
    public static final int TEMPLATE_ID = 1;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final MetricsEvent parentMessage = this;
    private DirectBuffer buffer;
    private int offset;
    private int limit;
    private int actingBlockLength;
    private int actingVersion;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public int offset()
    {
        return offset;
    }

    public MetricsEvent wrapForEncode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = BLOCK_LENGTH;
        this.actingVersion = SCHEMA_VERSION;
        limit(offset + actingBlockLength);

        return this;
    }

    public MetricsEvent wrapForDecode(
        final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

        return this;
    }

    public int size()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        buffer.checkLimit(limit);
        this.limit = limit;
    }

    public static int eventTypeId()
    {
        return 1;
    }

    public static String eventTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte eventTypeNullValue()
    {
        return (byte)-128;
    }

    public static byte eventTypeMinValue()
    {
        return (byte)-127;
    }

    public static byte eventTypeMaxValue()
    {
        return (byte)127;
    }

    public byte eventType()
    {
        return CodecUtil.int8Get(buffer, offset + 0);
    }

    public MetricsEvent eventType(final byte value)
    {
        CodecUtil.int8Put(buffer, offset + 0, value);
        return this;
    }

    public static int operationTypeId()
    {
        return 2;
    }

    public static String operationTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int operationTypeNullValue()
    {
        return -2147483648;
    }

    public static int operationTypeMinValue()
    {
        return -2147483647;
    }

    public static int operationTypeMaxValue()
    {
        return 2147483647;
    }

    public int operationType()
    {
        return CodecUtil.int32Get(buffer, offset + 1, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent operationType(final int value)
    {
        CodecUtil.int32Put(buffer, offset + 1, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int scheduledStartTimeAsMilliId()
    {
        return 3;
    }

    public static String scheduledStartTimeAsMilliMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long scheduledStartTimeAsMilliNullValue()
    {
        return -9223372036854775808L;
    }

    public static long scheduledStartTimeAsMilliMinValue()
    {
        return -9223372036854775807L;
    }

    public static long scheduledStartTimeAsMilliMaxValue()
    {
        return 9223372036854775807L;
    }

    public long scheduledStartTimeAsMilli()
    {
        return CodecUtil.int64Get(buffer, offset + 5, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent scheduledStartTimeAsMilli(final long value)
    {
        CodecUtil.int64Put(buffer, offset + 5, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int actualStartTimeAsMilliId()
    {
        return 4;
    }

    public static String actualStartTimeAsMilliMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long actualStartTimeAsMilliNullValue()
    {
        return -9223372036854775808L;
    }

    public static long actualStartTimeAsMilliMinValue()
    {
        return -9223372036854775807L;
    }

    public static long actualStartTimeAsMilliMaxValue()
    {
        return 9223372036854775807L;
    }

    public long actualStartTimeAsMilli()
    {
        return CodecUtil.int64Get(buffer, offset + 13, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent actualStartTimeAsMilli(final long value)
    {
        CodecUtil.int64Put(buffer, offset + 13, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int runDurationAsNanoId()
    {
        return 5;
    }

    public static String runDurationAsNanoMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long runDurationAsNanoNullValue()
    {
        return -9223372036854775808L;
    }

    public static long runDurationAsNanoMinValue()
    {
        return -9223372036854775807L;
    }

    public static long runDurationAsNanoMaxValue()
    {
        return 9223372036854775807L;
    }

    public long runDurationAsNano()
    {
        return CodecUtil.int64Get(buffer, offset + 21, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent runDurationAsNano(final long value)
    {
        CodecUtil.int64Put(buffer, offset + 21, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int resultCodeId()
    {
        return 6;
    }

    public static String resultCodeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int resultCodeNullValue()
    {
        return -2147483648;
    }

    public static int resultCodeMinValue()
    {
        return -2147483647;
    }

    public static int resultCodeMaxValue()
    {
        return 2147483647;
    }

    public int resultCode()
    {
        return CodecUtil.int32Get(buffer, offset + 29, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent resultCode(final int value)
    {
        CodecUtil.int32Put(buffer, offset + 29, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int originalStartTimeId()
    {
        return 7;
    }

    public static String originalStartTimeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long originalStartTimeNullValue()
    {
        return -9223372036854775808L;
    }

    public static long originalStartTimeMinValue()
    {
        return -9223372036854775807L;
    }

    public static long originalStartTimeMaxValue()
    {
        return 9223372036854775807L;
    }

    public long originalStartTime()
    {
        return CodecUtil.int64Get(buffer, offset + 33, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent originalStartTime(final long value)
    {
        CodecUtil.int64Put(buffer, offset + 33, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }
}

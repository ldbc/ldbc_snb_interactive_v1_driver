/* Generated SBE (Simple Binary Encoding) message codec */
package com.ldbc.driver.runtime.metrics.sbe;

import uk.co.real_logic.sbe.codec.java.CodecUtil;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

public class MetricsEvent
{
    public static final int BLOCK_LENGTH = 17;
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

    public static short eventTypeNullValue()
    {
        return (short)255;
    }

    public static short eventTypeMinValue()
    {
        return (short)0;
    }

    public static short eventTypeMaxValue()
    {
        return (short)254;
    }

    public short eventType()
    {
        return CodecUtil.uint8Get(buffer, offset + 0);
    }

    public MetricsEvent eventType(final short value)
    {
        CodecUtil.uint8Put(buffer, offset + 0, value);
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
        return 65535;
    }

    public static int operationTypeMinValue()
    {
        return 0;
    }

    public static int operationTypeMaxValue()
    {
        return 65534;
    }

    public int operationType()
    {
        return CodecUtil.uint16Get(buffer, offset + 1, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent operationType(final int value)
    {
        CodecUtil.uint16Put(buffer, offset + 1, value, java.nio.ByteOrder.LITTLE_ENDIAN);
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
        return 4294967294L;
    }

    public static long scheduledStartTimeAsMilliMinValue()
    {
        return 0L;
    }

    public static long scheduledStartTimeAsMilliMaxValue()
    {
        return 4294967293L;
    }

    public long scheduledStartTimeAsMilli()
    {
        return CodecUtil.uint32Get(buffer, offset + 3, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent scheduledStartTimeAsMilli(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 3, value, java.nio.ByteOrder.LITTLE_ENDIAN);
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
        return 4294967294L;
    }

    public static long actualStartTimeAsMilliMinValue()
    {
        return 0L;
    }

    public static long actualStartTimeAsMilliMaxValue()
    {
        return 4294967293L;
    }

    public long actualStartTimeAsMilli()
    {
        return CodecUtil.uint32Get(buffer, offset + 7, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent actualStartTimeAsMilli(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 7, value, java.nio.ByteOrder.LITTLE_ENDIAN);
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
        return 4294967294L;
    }

    public static long runDurationAsNanoMinValue()
    {
        return 0L;
    }

    public static long runDurationAsNanoMaxValue()
    {
        return 4294967293L;
    }

    public long runDurationAsNano()
    {
        return CodecUtil.uint32Get(buffer, offset + 11, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent runDurationAsNano(final long value)
    {
        CodecUtil.uint32Put(buffer, offset + 11, value, java.nio.ByteOrder.LITTLE_ENDIAN);
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
        return 65535;
    }

    public static int resultCodeMinValue()
    {
        return 0;
    }

    public static int resultCodeMaxValue()
    {
        return 65534;
    }

    public int resultCode()
    {
        return CodecUtil.uint16Get(buffer, offset + 15, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MetricsEvent resultCode(final int value)
    {
        CodecUtil.uint16Put(buffer, offset + 15, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }
}

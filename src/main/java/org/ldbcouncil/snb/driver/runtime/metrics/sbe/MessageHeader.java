/* Generated SBE (Simple Binary Encoding) message codec */
package org.ldbcouncil.snb.driver.runtime.metrics.sbe;

import org.agrona.concurrent.UnsafeBuffer;

public class MessageHeader
{
    private UnsafeBuffer buffer;
    private int offset;
    private int actingVersion;

    public MessageHeader wrap(final UnsafeBuffer buffer, final int offset, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingVersion = actingVersion;
        return this;
    }

    public int size()
    {
        return 8;
    }

    public static int blockLengthNullValue()
    {
        return 65535;
    }

    public static int blockLengthMinValue()
    {
        return 0;
    }

    public static int blockLengthMaxValue()
    {
        return 65534;
    }

    public int blockLength()
    {
        return buffer.getShort(offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MessageHeader blockLength(final short value)
    {
        buffer.putShort(offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int templateIdNullValue()
    {
        return 65535;
    }

    public static int templateIdMinValue()
    {
        return 0;
    }

    public static int templateIdMaxValue()
    {
        return 65534;
    }

    public int templateId()
    {
        return buffer.getShort(offset + 2, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MessageHeader templateId(final short value)
    {
        buffer.putShort(offset + 2, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int schemaIdNullValue()
    {
        return 65535;
    }

    public static int schemaIdMinValue()
    {
        return 0;
    }

    public static int schemaIdMaxValue()
    {
        return 65534;
    }

    public int schemaId()
    {
        return buffer.getShort(offset + 4, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MessageHeader schemaId(final short value)
    {
        buffer.putShort(offset + 4, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int versionNullValue()
    {
        return 65535;
    }

    public static int versionMinValue()
    {
        return 0;
    }

    public static int versionMaxValue()
    {
        return 65534;
    }

    public int version()
    {
        return buffer.getShort(offset + 6, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public MessageHeader version(final short value)
    {
        buffer.putShort(offset + 6, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }
}

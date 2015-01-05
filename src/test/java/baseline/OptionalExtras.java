/* Generated SBE (Simple Binary Encoding) message codec */
package baseline;

import uk.co.real_logic.sbe.codec.java.*;

public class OptionalExtras
{
    private DirectBuffer buffer;
    private int offset;
    private int actingVersion;

    public OptionalExtras wrap(final DirectBuffer buffer, final int offset, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingVersion = actingVersion;
        return this;
    }

    public int size()
    {
        return 1;
    }

    public OptionalExtras clear()
    {
        CodecUtil.uint8Put(buffer, offset, (short)0);
        return this;
    }

    public boolean sunRoof()
    {
        return CodecUtil.uint8GetChoice(buffer, offset, 0);
    }

    public OptionalExtras sunRoof(final boolean value)
    {
        CodecUtil.uint8PutChoice(buffer, offset, 0, value);
        return this;
    }

    public boolean sportsPack()
    {
        return CodecUtil.uint8GetChoice(buffer, offset, 1);
    }

    public OptionalExtras sportsPack(final boolean value)
    {
        CodecUtil.uint8PutChoice(buffer, offset, 1, value);
        return this;
    }

    public boolean cruiseControl()
    {
        return CodecUtil.uint8GetChoice(buffer, offset, 2);
    }

    public OptionalExtras cruiseControl(final boolean value)
    {
        CodecUtil.uint8PutChoice(buffer, offset, 2, value);
        return this;
    }
}

/* Generated SBE (Simple Binary Encoding) message codec */
package baseline;

import uk.co.real_logic.sbe.codec.java.*;

public class Car
{
    public static final int BLOCK_LENGTH = 45;
    public static final int TEMPLATE_ID = 1;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final Car parentMessage = this;
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

    public Car wrapForEncode(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = BLOCK_LENGTH;
        this.actingVersion = SCHEMA_VERSION;
        limit(offset + actingBlockLength);

        return this;
    }

    public Car wrapForDecode(
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

    public static int serialNumberId()
    {
        return 1;
    }

    public static String serialNumberMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static long serialNumberNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long serialNumberMinValue()
    {
        return 0x0L;
    }

    public static long serialNumberMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public long serialNumber()
    {
        return CodecUtil.uint64Get(buffer, offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public Car serialNumber(final long value)
    {
        CodecUtil.uint64Put(buffer, offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int modelYearId()
    {
        return 2;
    }

    public static String modelYearMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int modelYearNullValue()
    {
        return 65535;
    }

    public static int modelYearMinValue()
    {
        return 0;
    }

    public static int modelYearMaxValue()
    {
        return 65534;
    }

    public int modelYear()
    {
        return CodecUtil.uint16Get(buffer, offset + 8, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public Car modelYear(final int value)
    {
        CodecUtil.uint16Put(buffer, offset + 8, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }

    public static int availableId()
    {
        return 3;
    }

    public static String availableMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public BooleanType available()
    {
        return BooleanType.get(CodecUtil.uint8Get(buffer, offset + 10));
    }

    public Car available(final BooleanType value)
    {
        CodecUtil.uint8Put(buffer, offset + 10, value.value());
        return this;
    }

    public static int codeId()
    {
        return 4;
    }

    public static String codeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public Model code()
    {
        return Model.get(CodecUtil.charGet(buffer, offset + 11));
    }

    public Car code(final Model value)
    {
        CodecUtil.charPut(buffer, offset + 11, value.value());
        return this;
    }

    public static int someNumbersId()
    {
        return 5;
    }

    public static String someNumbersMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int someNumbersNullValue()
    {
        return -2147483648;
    }

    public static int someNumbersMinValue()
    {
        return -2147483647;
    }

    public static int someNumbersMaxValue()
    {
        return 2147483647;
    }

    public static int someNumbersLength()
    {
        return 5;
    }

    public int someNumbers(final int index)
    {
        if (index < 0 || index >= 5)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        return CodecUtil.int32Get(buffer, this.offset + 12 + (index * 4), java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public void someNumbers(final int index, final int value)
    {
        if (index < 0 || index >= 5)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        CodecUtil.int32Put(buffer, this.offset + 12 + (index * 4), value, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public static int vehicleCodeId()
    {
        return 6;
    }

    public static String vehicleCodeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static byte vehicleCodeNullValue()
    {
        return (byte)0;
    }

    public static byte vehicleCodeMinValue()
    {
        return (byte)32;
    }

    public static byte vehicleCodeMaxValue()
    {
        return (byte)126;
    }

    public static int vehicleCodeLength()
    {
        return 6;
    }

    public byte vehicleCode(final int index)
    {
        if (index < 0 || index >= 6)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        return CodecUtil.charGet(buffer, this.offset + 32 + (index * 1));
    }

    public void vehicleCode(final int index, final byte value)
    {
        if (index < 0 || index >= 6)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        CodecUtil.charPut(buffer, this.offset + 32 + (index * 1), value);
    }

    public static String vehicleCodeCharacterEncoding()
    {
        return "UTF-8";
    }

    public int getVehicleCode(final byte[] dst, final int dstOffset)
    {
        final int length = 6;
        if (dstOffset < 0 || dstOffset > (dst.length - length))
        {
            throw new IndexOutOfBoundsException(                "dstOffset out of range for copy: offset=" + dstOffset);
        }

        CodecUtil.charsGet(buffer, this.offset + 32, dst, dstOffset, length);
        return length;
    }

    public Car putVehicleCode(final byte[] src, final int srcOffset)
    {
        final int length = 6;
        if (srcOffset < 0 || srcOffset > (src.length - length))
        {
            throw new IndexOutOfBoundsException(                "srcOffset out of range for copy: offset=" + srcOffset);
        }

        CodecUtil.charsPut(buffer, this.offset + 32, src, srcOffset, length);
        return this;
    }

    public static int extrasId()
    {
        return 7;
    }

    public static String extrasMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    private final OptionalExtras extras = new OptionalExtras();

    public OptionalExtras extras()
    {
        extras.wrap(buffer, offset + 38, actingVersion);
        return extras;
    }

    public static int engineId()
    {
        return 8;
    }

    public static String engineMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    private final Engine engine = new Engine();

    public Engine engine()
    {
        engine.wrap(buffer, offset + 39, actingVersion);
        return engine;
    }

    private final FuelFigures fuelFigures = new FuelFigures();

    public static long fuelFiguresId()
    {
        return 9;
    }

    public FuelFigures fuelFigures()
    {
        fuelFigures.wrapForDecode(parentMessage, buffer, actingVersion);
        return fuelFigures;
    }

    public FuelFigures fuelFiguresCount(final int count)
    {
        fuelFigures.wrapForEncode(parentMessage, buffer, count);
        return fuelFigures;
    }

    public static class FuelFigures implements Iterable<FuelFigures>, java.util.Iterator<FuelFigures>
    {
        private static final int HEADER_SIZE = 3;
        private final GroupSizeEncoding dimensions = new GroupSizeEncoding();
        private Car parentMessage;
        private DirectBuffer buffer;
        private int blockLength;
        private int actingVersion;
        private int count;
        private int index;
        private int offset;

        public void wrapForDecode(
            final Car parentMessage, final DirectBuffer buffer, final int actingVersion)
        {
            this.parentMessage = parentMessage;
            this.buffer = buffer;
            dimensions.wrap(buffer, parentMessage.limit(), actingVersion);
            blockLength = dimensions.blockLength();
            count = dimensions.numInGroup();
            this.actingVersion = actingVersion;
            index = -1;
            parentMessage.limit(parentMessage.limit() + HEADER_SIZE);
        }

        public void wrapForEncode(final Car parentMessage, final DirectBuffer buffer, final int count)
        {
            this.parentMessage = parentMessage;
            this.buffer = buffer;
            actingVersion = SCHEMA_VERSION;
            dimensions.wrap(buffer, parentMessage.limit(), actingVersion);
            dimensions.blockLength((int)6);
            dimensions.numInGroup((short)count);
            index = -1;
            this.count = count;
            blockLength = 6;
            parentMessage.limit(parentMessage.limit() + HEADER_SIZE);
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 6;
        }

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int count()
        {
            return count;
        }

        @Override
        public java.util.Iterator<FuelFigures> iterator()
        {
            return this;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext()
        {
            return (index + 1) < count;
        }

        @Override
        public FuelFigures next()
        {
            if (index + 1 >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
        }

        public static int speedId()
        {
            return 10;
        }

        public static String speedMetaAttribute(final MetaAttribute metaAttribute)
        {
            switch (metaAttribute)
            {
                case EPOCH: return "unix";
                case TIME_UNIT: return "nanosecond";
                case SEMANTIC_TYPE: return "";
            }

            return "";
        }

        public static int speedNullValue()
        {
            return 65535;
        }

        public static int speedMinValue()
        {
            return 0;
        }

        public static int speedMaxValue()
        {
            return 65534;
        }

        public int speed()
        {
            return CodecUtil.uint16Get(buffer, offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
        }

        public FuelFigures speed(final int value)
        {
            CodecUtil.uint16Put(buffer, offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
            return this;
        }

        public static int mpgId()
        {
            return 11;
        }

        public static String mpgMetaAttribute(final MetaAttribute metaAttribute)
        {
            switch (metaAttribute)
            {
                case EPOCH: return "unix";
                case TIME_UNIT: return "nanosecond";
                case SEMANTIC_TYPE: return "";
            }

            return "";
        }

        public static float mpgNullValue()
        {
            return Float.NaN;
        }

        public static float mpgMinValue()
        {
            return 1.401298464324817E-45f;
        }

        public static float mpgMaxValue()
        {
            return 3.4028234663852886E38f;
        }

        public float mpg()
        {
            return CodecUtil.floatGet(buffer, offset + 2, java.nio.ByteOrder.LITTLE_ENDIAN);
        }

        public FuelFigures mpg(final float value)
        {
            CodecUtil.floatPut(buffer, offset + 2, value, java.nio.ByteOrder.LITTLE_ENDIAN);
            return this;
        }
    }

    private final PerformanceFigures performanceFigures = new PerformanceFigures();

    public static long performanceFiguresId()
    {
        return 12;
    }

    public PerformanceFigures performanceFigures()
    {
        performanceFigures.wrapForDecode(parentMessage, buffer, actingVersion);
        return performanceFigures;
    }

    public PerformanceFigures performanceFiguresCount(final int count)
    {
        performanceFigures.wrapForEncode(parentMessage, buffer, count);
        return performanceFigures;
    }

    public static class PerformanceFigures implements Iterable<PerformanceFigures>, java.util.Iterator<PerformanceFigures>
    {
        private static final int HEADER_SIZE = 3;
        private final GroupSizeEncoding dimensions = new GroupSizeEncoding();
        private Car parentMessage;
        private DirectBuffer buffer;
        private int blockLength;
        private int actingVersion;
        private int count;
        private int index;
        private int offset;

        public void wrapForDecode(
            final Car parentMessage, final DirectBuffer buffer, final int actingVersion)
        {
            this.parentMessage = parentMessage;
            this.buffer = buffer;
            dimensions.wrap(buffer, parentMessage.limit(), actingVersion);
            blockLength = dimensions.blockLength();
            count = dimensions.numInGroup();
            this.actingVersion = actingVersion;
            index = -1;
            parentMessage.limit(parentMessage.limit() + HEADER_SIZE);
        }

        public void wrapForEncode(final Car parentMessage, final DirectBuffer buffer, final int count)
        {
            this.parentMessage = parentMessage;
            this.buffer = buffer;
            actingVersion = SCHEMA_VERSION;
            dimensions.wrap(buffer, parentMessage.limit(), actingVersion);
            dimensions.blockLength((int)1);
            dimensions.numInGroup((short)count);
            index = -1;
            this.count = count;
            blockLength = 1;
            parentMessage.limit(parentMessage.limit() + HEADER_SIZE);
        }

        public static int sbeHeaderSize()
        {
            return HEADER_SIZE;
        }

        public static int sbeBlockLength()
        {
            return 1;
        }

        public int actingBlockLength()
        {
            return blockLength;
        }

        public int count()
        {
            return count;
        }

        @Override
        public java.util.Iterator<PerformanceFigures> iterator()
        {
            return this;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext()
        {
            return (index + 1) < count;
        }

        @Override
        public PerformanceFigures next()
        {
            if (index + 1 >= count)
            {
                throw new java.util.NoSuchElementException();
            }

            offset = parentMessage.limit();
            parentMessage.limit(offset + blockLength);
            ++index;

            return this;
        }

        public static int octaneRatingId()
        {
            return 13;
        }

        public static String octaneRatingMetaAttribute(final MetaAttribute metaAttribute)
        {
            switch (metaAttribute)
            {
                case EPOCH: return "unix";
                case TIME_UNIT: return "nanosecond";
                case SEMANTIC_TYPE: return "";
            }

            return "";
        }

        public static short octaneRatingNullValue()
        {
            return (short)255;
        }

        public static short octaneRatingMinValue()
        {
            return (short)90;
        }

        public static short octaneRatingMaxValue()
        {
            return (short)110;
        }

        public short octaneRating()
        {
            return CodecUtil.uint8Get(buffer, offset + 0);
        }

        public PerformanceFigures octaneRating(final short value)
        {
            CodecUtil.uint8Put(buffer, offset + 0, value);
            return this;
        }

        private final Acceleration acceleration = new Acceleration();

        public static long accelerationId()
        {
            return 14;
        }

        public Acceleration acceleration()
        {
            acceleration.wrapForDecode(parentMessage, buffer, actingVersion);
            return acceleration;
        }

        public Acceleration accelerationCount(final int count)
        {
            acceleration.wrapForEncode(parentMessage, buffer, count);
            return acceleration;
        }

        public static class Acceleration implements Iterable<Acceleration>, java.util.Iterator<Acceleration>
        {
            private static final int HEADER_SIZE = 3;
            private final GroupSizeEncoding dimensions = new GroupSizeEncoding();
            private Car parentMessage;
            private DirectBuffer buffer;
            private int blockLength;
            private int actingVersion;
            private int count;
            private int index;
            private int offset;

            public void wrapForDecode(
                final Car parentMessage, final DirectBuffer buffer, final int actingVersion)
            {
                this.parentMessage = parentMessage;
                this.buffer = buffer;
                dimensions.wrap(buffer, parentMessage.limit(), actingVersion);
                blockLength = dimensions.blockLength();
                count = dimensions.numInGroup();
                this.actingVersion = actingVersion;
                index = -1;
                parentMessage.limit(parentMessage.limit() + HEADER_SIZE);
            }

            public void wrapForEncode(final Car parentMessage, final DirectBuffer buffer, final int count)
            {
                this.parentMessage = parentMessage;
                this.buffer = buffer;
                actingVersion = SCHEMA_VERSION;
                dimensions.wrap(buffer, parentMessage.limit(), actingVersion);
                dimensions.blockLength((int)6);
                dimensions.numInGroup((short)count);
                index = -1;
                this.count = count;
                blockLength = 6;
                parentMessage.limit(parentMessage.limit() + HEADER_SIZE);
            }

            public static int sbeHeaderSize()
            {
                return HEADER_SIZE;
            }

            public static int sbeBlockLength()
            {
                return 6;
            }

            public int actingBlockLength()
            {
                return blockLength;
            }

            public int count()
            {
                return count;
            }

            @Override
            public java.util.Iterator<Acceleration> iterator()
            {
                return this;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext()
            {
                return (index + 1) < count;
            }

            @Override
            public Acceleration next()
            {
                if (index + 1 >= count)
                {
                    throw new java.util.NoSuchElementException();
                }

                offset = parentMessage.limit();
                parentMessage.limit(offset + blockLength);
                ++index;

                return this;
            }

            public static int mphId()
            {
                return 15;
            }

            public static String mphMetaAttribute(final MetaAttribute metaAttribute)
            {
                switch (metaAttribute)
                {
                    case EPOCH: return "unix";
                    case TIME_UNIT: return "nanosecond";
                    case SEMANTIC_TYPE: return "";
                }

                return "";
            }

            public static int mphNullValue()
            {
                return 65535;
            }

            public static int mphMinValue()
            {
                return 0;
            }

            public static int mphMaxValue()
            {
                return 65534;
            }

            public int mph()
            {
                return CodecUtil.uint16Get(buffer, offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN);
            }

            public Acceleration mph(final int value)
            {
                CodecUtil.uint16Put(buffer, offset + 0, value, java.nio.ByteOrder.LITTLE_ENDIAN);
                return this;
            }

            public static int secondsId()
            {
                return 16;
            }

            public static String secondsMetaAttribute(final MetaAttribute metaAttribute)
            {
                switch (metaAttribute)
                {
                    case EPOCH: return "unix";
                    case TIME_UNIT: return "nanosecond";
                    case SEMANTIC_TYPE: return "";
                }

                return "";
            }

            public static float secondsNullValue()
            {
                return Float.NaN;
            }

            public static float secondsMinValue()
            {
                return 1.401298464324817E-45f;
            }

            public static float secondsMaxValue()
            {
                return 3.4028234663852886E38f;
            }

            public float seconds()
            {
                return CodecUtil.floatGet(buffer, offset + 2, java.nio.ByteOrder.LITTLE_ENDIAN);
            }

            public Acceleration seconds(final float value)
            {
                CodecUtil.floatPut(buffer, offset + 2, value, java.nio.ByteOrder.LITTLE_ENDIAN);
                return this;
            }
        }
    }

    public static int makeId()
    {
        return 17;
    }

    public static String makeCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String makeMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int makeHeaderSize()
    {
        return 1;
    }

    public int getMake(final byte[] dst, final int dstOffset, final int length)
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        buffer.checkLimit(limit + sizeOfLengthField);
        final int dataLength = CodecUtil.uint8Get(buffer, limit);
        final int bytesCopied = Math.min(length, dataLength);
        limit(limit + sizeOfLengthField + dataLength);
        CodecUtil.int8sGet(buffer, limit + sizeOfLengthField, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int putMake(final byte[] src, final int srcOffset, final int length)
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        limit(limit + sizeOfLengthField + length);
        CodecUtil.uint8Put(buffer, limit, (short)length);
        CodecUtil.int8sPut(buffer, limit + sizeOfLengthField, src, srcOffset, length);

        return length;
    }

    public static int modelId()
    {
        return 18;
    }

    public static String modelCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String modelMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int modelHeaderSize()
    {
        return 1;
    }

    public int getModel(final byte[] dst, final int dstOffset, final int length)
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        buffer.checkLimit(limit + sizeOfLengthField);
        final int dataLength = CodecUtil.uint8Get(buffer, limit);
        final int bytesCopied = Math.min(length, dataLength);
        limit(limit + sizeOfLengthField + dataLength);
        CodecUtil.int8sGet(buffer, limit + sizeOfLengthField, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int putModel(final byte[] src, final int srcOffset, final int length)
    {
        final int sizeOfLengthField = 1;
        final int limit = limit();
        limit(limit + sizeOfLengthField + length);
        CodecUtil.uint8Put(buffer, limit, (short)length);
        CodecUtil.int8sPut(buffer, limit + sizeOfLengthField, src, srcOffset, length);

        return length;
    }
}

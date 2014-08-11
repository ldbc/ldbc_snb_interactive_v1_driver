package com.ldbc.driver.generator;

import org.apache.commons.math3.random.RandomDataGenerator;

public class UniformByteGenerator extends Generator<Byte> {
    private final RandomDataGenerator randomDataGenerator;
    private final byte[] buffer;
    private int currentIndexInBuffer;

    UniformByteGenerator(RandomDataGenerator randomDataGenerator) {
        this.randomDataGenerator = randomDataGenerator;
        this.buffer = new byte[8];
        this.currentIndexInBuffer = this.buffer.length;
        fillBufferIfEmpty();
    }

    private void fillBufferIfEmpty() {
        if (currentIndexInBuffer == buffer.length) {
            doFillBufferWithBytesFromRandomLong(buffer);
            currentIndexInBuffer = 0;
        }
    }

    private void doFillBufferWithBytesFromRandomLong(byte[] buffer) {
        long randomBytes = randomDataGenerator.nextLong(Long.MIN_VALUE, Long.MAX_VALUE);
        buffer[0] = (byte) ((randomBytes >> 0) & 255);
        buffer[1] = (byte) ((randomBytes >> 8) & 255);
        buffer[2] = (byte) ((randomBytes >> 16) & 255);
        buffer[3] = (byte) ((randomBytes >> 24) & 255);
        buffer[4] = (byte) ((randomBytes >> 32) & 255);
        buffer[5] = (byte) ((randomBytes >> 40) & 255);
        buffer[6] = (byte) ((randomBytes >> 48) & 255);
        buffer[7] = (byte) ((randomBytes >> 56) & 255);
    }

    @Override
    protected Byte doNext() throws GeneratorException {
        fillBufferIfEmpty();
        return buffer[currentIndexInBuffer++];
    }
}

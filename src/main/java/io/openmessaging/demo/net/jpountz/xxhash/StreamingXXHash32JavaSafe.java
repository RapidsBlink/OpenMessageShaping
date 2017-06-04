// Auto-generated: DO NOT EDIT

package io.openmessaging.demo.net.jpountz.xxhash;

import io.openmessaging.demo.net.jpountz.util.SafeUtils;

import static java.lang.Integer.rotateLeft;

/**
 * Streaming xxhash.
 */
final class StreamingXXHash32JavaSafe extends AbstractStreamingXXHash32Java {

  static class Factory implements StreamingXXHash32.Factory {

    public static final StreamingXXHash32.Factory INSTANCE = new Factory();

    @Override
    public StreamingXXHash32 newStreamingHash(int seed) {
      return new StreamingXXHash32JavaSafe(seed);
    }

  }

  StreamingXXHash32JavaSafe(int seed) {
    super(seed);
  }

  @Override
  public int getValue() {
    int h32;
    if (totalLen >= 16) {
      h32 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
    } else {
      h32 = seed + XXHashConstants.PRIME5;
    }

    h32 += totalLen;

    int off = 0;
    while (off <= memSize - 4) {
      h32 += SafeUtils.readIntLE(memory, off) * XXHashConstants.PRIME3;
      h32 = rotateLeft(h32, 17) * XXHashConstants.PRIME4;
      off += 4;
    }

    while (off < memSize) {
      h32 += (SafeUtils.readByte(memory, off) & 0xFF) * XXHashConstants.PRIME5;
      h32 = rotateLeft(h32, 11) * XXHashConstants.PRIME1;
      ++off;
    }

    h32 ^= h32 >>> 15;
    h32 *= XXHashConstants.PRIME2;
    h32 ^= h32 >>> 13;
    h32 *= XXHashConstants.PRIME3;
    h32 ^= h32 >>> 16;

    return h32;
  }

  @Override
  public void update(byte[] buf, int off, int len) {
    SafeUtils.checkRange(buf, off, len);

    totalLen += len;

    if (memSize + len < 16) { // fill in tmp buffer
      System.arraycopy(buf, off, memory, memSize, len);
      memSize += len;
      return;
    }

    final int end = off + len;

    if (memSize > 0) { // data left from previous update
      System.arraycopy(buf, off, memory, memSize, 16 - memSize);

      v1 += SafeUtils.readIntLE(memory, 0) * XXHashConstants.PRIME2;
      v1 = rotateLeft(v1, 13);
      v1 *= XXHashConstants.PRIME1;

      v2 += SafeUtils.readIntLE(memory, 4) * XXHashConstants.PRIME2;
      v2 = rotateLeft(v2, 13);
      v2 *= XXHashConstants.PRIME1;

      v3 += SafeUtils.readIntLE(memory, 8) * XXHashConstants.PRIME2;
      v3 = rotateLeft(v3, 13);
      v3 *= XXHashConstants.PRIME1;

      v4 += SafeUtils.readIntLE(memory, 12) * XXHashConstants.PRIME2;
      v4 = rotateLeft(v4, 13);
      v4 *= XXHashConstants.PRIME1;

      off += 16 - memSize;
      memSize = 0;
    }

    {
      final int limit = end - 16;
      int v1 = this.v1;
      int v2 = this.v2;
      int v3 = this.v3;
      int v4 = this.v4;

      while (off <= limit) {
        v1 += SafeUtils.readIntLE(buf, off) * XXHashConstants.PRIME2;
        v1 = rotateLeft(v1, 13);
        v1 *= XXHashConstants.PRIME1;
        off += 4;

        v2 += SafeUtils.readIntLE(buf, off) * XXHashConstants.PRIME2;
        v2 = rotateLeft(v2, 13);
        v2 *= XXHashConstants.PRIME1;
        off += 4;

        v3 += SafeUtils.readIntLE(buf, off) * XXHashConstants.PRIME2;
        v3 = rotateLeft(v3, 13);
        v3 *= XXHashConstants.PRIME1;
        off += 4;

        v4 += SafeUtils.readIntLE(buf, off) * XXHashConstants.PRIME2;
        v4 = rotateLeft(v4, 13);
        v4 *= XXHashConstants.PRIME1;
        off += 4;
      }

      this.v1 = v1;
      this.v2 = v2;
      this.v3 = v3;
      this.v4 = v4;
    }

    if (off < end) {
      System.arraycopy(buf, off, memory, 0, end - off);
      memSize = end - off;
    }
  }

}


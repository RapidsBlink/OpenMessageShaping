// Auto-generated: DO NOT EDIT

package io.openmessaging.demo.net.jpountz.lz4;

import io.openmessaging.demo.net.jpountz.util.ByteBufferUtils;
import io.openmessaging.demo.net.jpountz.util.UnsafeUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * High compression compressor.
 */
final class LZ4HCJavaUnsafeCompressor extends LZ4Compressor {

  public static final LZ4Compressor INSTANCE = new LZ4HCJavaUnsafeCompressor();

  private final int maxAttempts;
  final int compressionLevel;
  
  LZ4HCJavaUnsafeCompressor() { this(LZ4Constants.DEFAULT_COMPRESSION_LEVEL); }
  LZ4HCJavaUnsafeCompressor(int compressionLevel) {
    this.maxAttempts = 1<<(compressionLevel-1);
    this.compressionLevel = compressionLevel;
  }

  private class HashTable {
    static final int MASK = LZ4Constants.MAX_DISTANCE - 1;
    int nextToUpdate;
    private final int base;
    private final int[] hashTable;
    private final short[] chainTable;

    HashTable(int base) {
      this.base = base;
      nextToUpdate = base;
      hashTable = new int[LZ4Constants.HASH_TABLE_SIZE_HC];
      Arrays.fill(hashTable, -1);
      chainTable = new short[LZ4Constants.MAX_DISTANCE];
    }

    private int hashPointer(byte[] bytes, int off) {
      final int v = UnsafeUtils.readInt(bytes, off);
      return hashPointer(v);
    }

    private int hashPointer(ByteBuffer bytes, int off) {
      final int v = ByteBufferUtils.readInt(bytes, off);
      return hashPointer(v);
    }

    private int hashPointer(int v) {
      final int h = LZ4Utils.hashHC(v);
      return hashTable[h];
    }

    private int next(int off) {
      return off - (chainTable[off & MASK] & 0xFFFF);
    }

    private void addHash(byte[] bytes, int off) {
      final int v = UnsafeUtils.readInt(bytes, off);
      addHash(v, off);
    }

    private void addHash(ByteBuffer bytes, int off) {
      final int v = ByteBufferUtils.readInt(bytes, off);
      addHash(v, off);
    }

    private void addHash(int v, int off) {
      final int h = LZ4Utils.hashHC(v);
      int delta = off - hashTable[h];
      assert delta > 0 : delta;
      if (delta >= LZ4Constants.MAX_DISTANCE) {
        delta = LZ4Constants.MAX_DISTANCE - 1;
      }
      chainTable[off & MASK] = (short) delta;
      hashTable[h] = off;
    }

    void insert(int off, byte[] bytes) {
      for (; nextToUpdate < off; ++nextToUpdate) {
        addHash(bytes, nextToUpdate);
      }
    }

    void insert(int off, ByteBuffer bytes) {
      for (; nextToUpdate < off; ++nextToUpdate) {
        addHash(bytes, nextToUpdate);
      } 
    }



    boolean insertAndFindBestMatch(byte[] buf, int off, int matchLimit, LZ4Utils.Match match) {
      match.start = off;
      match.len = 0;
      int delta = 0;
      int repl = 0;

      insert(off, buf);

      int ref = hashPointer(buf, off);

      if (ref >= off - 4 && ref <= off && ref >= base) { // potential repetition
        if (LZ4UnsafeUtils.readIntEquals(buf, ref, off)) { // confirmed
          delta = off - ref;
          repl = match.len = LZ4Constants.MIN_MATCH + LZ4UnsafeUtils.commonBytes(buf, ref + LZ4Constants.MIN_MATCH, off + LZ4Constants.MIN_MATCH, matchLimit);
          match.ref = ref;
        }
        ref = next(ref);
      }

      for (int i = 0; i < maxAttempts; ++i) {
        if (ref < Math.max(base, off - LZ4Constants.MAX_DISTANCE + 1) || ref > off) {
          break;
        }
        if (LZ4UnsafeUtils.readIntEquals(buf, ref, off)) {
          final int matchLen = LZ4Constants.MIN_MATCH + LZ4UnsafeUtils.commonBytes(buf, ref + LZ4Constants.MIN_MATCH, off + LZ4Constants.MIN_MATCH, matchLimit);
          if (matchLen > match.len) {
            match.ref = ref;
            match.len = matchLen;
          }
        }
        ref = next(ref);
      }

      if (repl != 0) {
        int ptr = off;
        final int end = off + repl - (LZ4Constants.MIN_MATCH - 1);
        while (ptr < end - delta) {
          chainTable[ptr & MASK] = (short) delta; // pre load
          ++ptr;
        }
        do {
          chainTable[ptr & MASK] = (short) delta;
          hashTable[LZ4Utils.hashHC(UnsafeUtils.readInt(buf, ptr))] = ptr;
          ++ptr;
        } while (ptr < end);
        nextToUpdate = end;
      }

      return match.len != 0;
    }

    boolean insertAndFindWiderMatch(byte[] buf, int off, int startLimit, int matchLimit, int minLen, LZ4Utils.Match match) {
      match.len = minLen;

      insert(off, buf);

      final int delta = off - startLimit;
      int ref = hashPointer(buf, off);
      for (int i = 0; i < maxAttempts; ++i) {
        if (ref < Math.max(base, off - LZ4Constants.MAX_DISTANCE + 1) || ref > off) {
          break;
        }
        if (LZ4UnsafeUtils.readIntEquals(buf, ref, off)) {
          final int matchLenForward = LZ4Constants.MIN_MATCH +LZ4UnsafeUtils.commonBytes(buf, ref + LZ4Constants.MIN_MATCH, off + LZ4Constants.MIN_MATCH, matchLimit);
          final int matchLenBackward = LZ4UnsafeUtils.commonBytesBackward(buf, ref, off, base, startLimit);
          final int matchLen = matchLenBackward + matchLenForward;
          if (matchLen > match.len) {
            match.len = matchLen;
            match.ref = ref - matchLenBackward;
            match.start = off - matchLenBackward;
          }
        }
        ref = next(ref);
      }

      return match.len > minLen;
    }


    boolean insertAndFindBestMatch(ByteBuffer buf, int off, int matchLimit, LZ4Utils.Match match) {
      match.start = off;
      match.len = 0;
      int delta = 0;
      int repl = 0;

      insert(off, buf);

      int ref = hashPointer(buf, off);

      if (ref >= off - 4 && ref <= off && ref >= base) { // potential repetition
        if (LZ4ByteBufferUtils.readIntEquals(buf, ref, off)) { // confirmed
          delta = off - ref;
          repl = match.len = LZ4Constants.MIN_MATCH + LZ4ByteBufferUtils.commonBytes(buf, ref + LZ4Constants.MIN_MATCH, off + LZ4Constants.MIN_MATCH, matchLimit);
          match.ref = ref;
        }
        ref = next(ref);
      }

      for (int i = 0; i < maxAttempts; ++i) {
        if (ref < Math.max(base, off - LZ4Constants.MAX_DISTANCE + 1) || ref > off) {
          break;
        }
        if (LZ4ByteBufferUtils.readIntEquals(buf, ref, off)) {
          final int matchLen = LZ4Constants.MIN_MATCH + LZ4ByteBufferUtils.commonBytes(buf, ref + LZ4Constants.MIN_MATCH, off + LZ4Constants.MIN_MATCH, matchLimit);
          if (matchLen > match.len) {
            match.ref = ref;
            match.len = matchLen;
          }
        }
        ref = next(ref);
      }

      if (repl != 0) {
        int ptr = off;
        final int end = off + repl - (LZ4Constants.MIN_MATCH - 1);
        while (ptr < end - delta) {
          chainTable[ptr & MASK] = (short) delta; // pre load
          ++ptr;
        }
        do {
          chainTable[ptr & MASK] = (short) delta;
          hashTable[LZ4Utils.hashHC(ByteBufferUtils.readInt(buf, ptr))] = ptr;
          ++ptr;
        } while (ptr < end);
        nextToUpdate = end;
      }

      return match.len != 0;
    }

    boolean insertAndFindWiderMatch(ByteBuffer buf, int off, int startLimit, int matchLimit, int minLen, LZ4Utils.Match match) {
      match.len = minLen;

      insert(off, buf);

      final int delta = off - startLimit;
      int ref = hashPointer(buf, off);
      for (int i = 0; i < maxAttempts; ++i) {
        if (ref < Math.max(base, off - LZ4Constants.MAX_DISTANCE + 1) || ref > off) {
          break;
        }
        if (LZ4ByteBufferUtils.readIntEquals(buf, ref, off)) {
          final int matchLenForward = LZ4Constants.MIN_MATCH + LZ4ByteBufferUtils.commonBytes(buf, ref + LZ4Constants.MIN_MATCH, off + LZ4Constants.MIN_MATCH, matchLimit);
          final int matchLenBackward = LZ4ByteBufferUtils.commonBytesBackward(buf, ref, off, base, startLimit);
          final int matchLen = matchLenBackward + matchLenForward;
          if (matchLen > match.len) {
            match.len = matchLen;
            match.ref = ref - matchLenBackward;
            match.start = off - matchLenBackward;
          }
        }
        ref = next(ref);
      }

      return match.len > minLen;
    }


  }

  @Override
  public int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen) {

    UnsafeUtils.checkRange(src, srcOff, srcLen);
    UnsafeUtils.checkRange(dest, destOff, maxDestLen);

    final int srcEnd = srcOff + srcLen;
    final int destEnd = destOff + maxDestLen;
    final int mfLimit = srcEnd - LZ4Constants.MF_LIMIT;
    final int matchLimit = srcEnd - LZ4Constants.LAST_LITERALS;

    int sOff = srcOff;
    int dOff = destOff;
    int anchor = sOff++;

    final HashTable ht = new HashTable(srcOff);
    final LZ4Utils.Match match0 = new LZ4Utils.Match();
    final LZ4Utils.Match match1 = new LZ4Utils.Match();
    final LZ4Utils.Match match2 = new LZ4Utils.Match();
    final LZ4Utils.Match match3 = new LZ4Utils.Match();

    main:
    while (sOff < mfLimit) {
      if (!ht.insertAndFindBestMatch(src, sOff, matchLimit, match1)) {
        ++sOff;
        continue;
      }

      // saved, in case we would skip too much
      LZ4Utils.copyTo(match1, match0);

      search2:
      while (true) {
        assert match1.start >= anchor;
        if (match1.end() >= mfLimit
            || !ht.insertAndFindWiderMatch(src, match1.end() - 2, match1.start + 1, matchLimit, match1.len, match2)) {
          // no better match
          dOff = LZ4UnsafeUtils.encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
          anchor = sOff = match1.end();
          continue main;
        }

        if (match0.start < match1.start) {
          if (match2.start < match1.start + match0.len) { // empirical
            LZ4Utils.copyTo(match0, match1);
          }
        }
        assert match2.start > match1.start;

        if (match2.start - match1.start < 3) { // First Match too small : removed
          LZ4Utils.copyTo(match2, match1);
          continue search2;
        }

        search3:
        while (true) {
          if (match2.start - match1.start < LZ4Constants.OPTIMAL_ML) {
            int newMatchLen = match1.len;
            if (newMatchLen > LZ4Constants.OPTIMAL_ML) {
              newMatchLen = LZ4Constants.OPTIMAL_ML;
            }
            if (match1.start + newMatchLen > match2.end() - LZ4Constants.MIN_MATCH) {
              newMatchLen = match2.start - match1.start + match2.len - LZ4Constants.MIN_MATCH;
            }
            final int correction = newMatchLen - (match2.start - match1.start);
            if (correction > 0) {
              match2.fix(correction);
            }
          }

          if (match2.start + match2.len >= mfLimit
              || !ht.insertAndFindWiderMatch(src, match2.end() - 3, match2.start, matchLimit, match2.len, match3)) {
            // no better match -> 2 sequences to encode
            if (match2.start < match1.end()) {
              match1.len = match2.start - match1.start;
            }
            // encode seq 1
            dOff = LZ4UnsafeUtils.encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
            anchor = sOff = match1.end();
            // encode seq 2
            dOff = LZ4UnsafeUtils.encodeSequence(src, anchor, match2.start, match2.ref, match2.len, dest, dOff, destEnd);
            anchor = sOff = match2.end();
            continue main;
          }

          if (match3.start < match1.end() + 3) { // Not enough space for match 2 : remove it
            if (match3.start >= match1.end()) { // // can write Seq1 immediately ==> Seq2 is removed, so Seq3 becomes Seq1
              if (match2.start < match1.end()) {
                final int correction = match1.end() - match2.start;
                match2.fix(correction);
                if (match2.len < LZ4Constants.MIN_MATCH) {
                  LZ4Utils.copyTo(match3, match2);
                }
              }

              dOff = LZ4UnsafeUtils.encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
              anchor = sOff = match1.end();

              LZ4Utils.copyTo(match3, match1);
              LZ4Utils.copyTo(match2, match0);

              continue search2;
            }

            LZ4Utils.copyTo(match3, match2);
            continue search3;
          }

          // OK, now we have 3 ascending matches; let's write at least the first one
          if (match2.start < match1.end()) {
            if (match2.start - match1.start < LZ4Constants.ML_MASK) {
              if (match1.len > LZ4Constants.OPTIMAL_ML) {
                match1.len = LZ4Constants.OPTIMAL_ML;
              }
              if (match1.end() > match2.end() - LZ4Constants.MIN_MATCH) {
                match1.len = match2.end() - match1.start - LZ4Constants.MIN_MATCH;
              }
              final int correction = match1.end() - match2.start;
              match2.fix(correction);
            } else {
              match1.len = match2.start - match1.start;
            }
          }

          dOff = LZ4UnsafeUtils.encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
          anchor = sOff = match1.end();

          LZ4Utils.copyTo(match2, match1);
          LZ4Utils.copyTo(match3, match2);

          continue search3;
        }

      }

    }

    dOff = LZ4UnsafeUtils.lastLiterals(src, anchor, srcEnd - anchor, dest, dOff, destEnd);
    return dOff - destOff;
  }


  @Override
  public int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen) {

    if (src.hasArray() && dest.hasArray()) {
      return compress(src.array(), srcOff + src.arrayOffset(), srcLen, dest.array(), destOff + dest.arrayOffset(), maxDestLen);
    }
    src = ByteBufferUtils.inNativeByteOrder(src);
    dest = ByteBufferUtils.inNativeByteOrder(dest);

    ByteBufferUtils.checkRange(src, srcOff, srcLen);
    ByteBufferUtils.checkRange(dest, destOff, maxDestLen);

    final int srcEnd = srcOff + srcLen;
    final int destEnd = destOff + maxDestLen;
    final int mfLimit = srcEnd - LZ4Constants.MF_LIMIT;
    final int matchLimit = srcEnd - LZ4Constants.LAST_LITERALS;

    int sOff = srcOff;
    int dOff = destOff;
    int anchor = sOff++;

    final HashTable ht = new HashTable(srcOff);
    final LZ4Utils.Match match0 = new LZ4Utils.Match();
    final LZ4Utils.Match match1 = new LZ4Utils.Match();
    final LZ4Utils.Match match2 = new LZ4Utils.Match();
    final LZ4Utils.Match match3 = new LZ4Utils.Match();

    main:
    while (sOff < mfLimit) {
      if (!ht.insertAndFindBestMatch(src, sOff, matchLimit, match1)) {
        ++sOff;
        continue;
      }

      // saved, in case we would skip too much
      LZ4Utils.copyTo(match1, match0);

      search2:
      while (true) {
        assert match1.start >= anchor;
        if (match1.end() >= mfLimit
            || !ht.insertAndFindWiderMatch(src, match1.end() - 2, match1.start + 1, matchLimit, match1.len, match2)) {
          // no better match
          dOff = LZ4ByteBufferUtils.encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
          anchor = sOff = match1.end();
          continue main;
        }

        if (match0.start < match1.start) {
          if (match2.start < match1.start + match0.len) { // empirical
            LZ4Utils.copyTo(match0, match1);
          }
        }
        assert match2.start > match1.start;

        if (match2.start - match1.start < 3) { // First Match too small : removed
          LZ4Utils.copyTo(match2, match1);
          continue search2;
        }

        search3:
        while (true) {
          if (match2.start - match1.start < LZ4Constants.OPTIMAL_ML) {
            int newMatchLen = match1.len;
            if (newMatchLen > LZ4Constants.OPTIMAL_ML) {
              newMatchLen = LZ4Constants.OPTIMAL_ML;
            }
            if (match1.start + newMatchLen > match2.end() - LZ4Constants.MIN_MATCH) {
              newMatchLen = match2.start - match1.start + match2.len - LZ4Constants.MIN_MATCH;
            }
            final int correction = newMatchLen - (match2.start - match1.start);
            if (correction > 0) {
              match2.fix(correction);
            }
          }

          if (match2.start + match2.len >= mfLimit
              || !ht.insertAndFindWiderMatch(src, match2.end() - 3, match2.start, matchLimit, match2.len, match3)) {
            // no better match -> 2 sequences to encode
            if (match2.start < match1.end()) {
              match1.len = match2.start - match1.start;
            }
            // encode seq 1
            dOff = LZ4ByteBufferUtils.encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
            anchor = sOff = match1.end();
            // encode seq 2
            dOff = LZ4ByteBufferUtils.encodeSequence(src, anchor, match2.start, match2.ref, match2.len, dest, dOff, destEnd);
            anchor = sOff = match2.end();
            continue main;
          }

          if (match3.start < match1.end() + 3) { // Not enough space for match 2 : remove it
            if (match3.start >= match1.end()) { // // can write Seq1 immediately ==> Seq2 is removed, so Seq3 becomes Seq1
              if (match2.start < match1.end()) {
                final int correction = match1.end() - match2.start;
                match2.fix(correction);
                if (match2.len < LZ4Constants.MIN_MATCH) {
                  LZ4Utils.copyTo(match3, match2);
                }
              }

              dOff = LZ4ByteBufferUtils.encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
              anchor = sOff = match1.end();

              LZ4Utils.copyTo(match3, match1);
              LZ4Utils.copyTo(match2, match0);

              continue search2;
            }

            LZ4Utils.copyTo(match3, match2);
            continue search3;
          }

          // OK, now we have 3 ascending matches; let's write at least the first one
          if (match2.start < match1.end()) {
            if (match2.start - match1.start < LZ4Constants.ML_MASK) {
              if (match1.len > LZ4Constants.OPTIMAL_ML) {
                match1.len = LZ4Constants.OPTIMAL_ML;
              }
              if (match1.end() > match2.end() - LZ4Constants.MIN_MATCH) {
                match1.len = match2.end() - match1.start - LZ4Constants.MIN_MATCH;
              }
              final int correction = match1.end() - match2.start;
              match2.fix(correction);
            } else {
              match1.len = match2.start - match1.start;
            }
          }

          dOff = LZ4ByteBufferUtils.encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
          anchor = sOff = match1.end();

          LZ4Utils.copyTo(match2, match1);
          LZ4Utils.copyTo(match3, match2);

          continue search3;
        }

      }

    }

    dOff = LZ4ByteBufferUtils.lastLiterals(src, anchor, srcEnd - anchor, dest, dOff, destEnd);
    return dOff - destOff;
  }


}

#!/usr/bin/env python3
"""Generate the shared payload blob used by all benchmark write programs.

Every language's write benchmark slices its message payloads out of this
one file, so the input data is byte-identical across implementations by
construction. The blob is deterministic (fixed seed), so results are
reproducible across runs and machines.

The data is shaped to compress like real robot sensor data rather than
sitting at either extreme (a repeating pattern compresses to nearly
nothing; pure random data doesn't compress at all). It is a stream of
little-endian 16-bit "sensor samples": a slowly-varying triangle wave
with random noise added to roughly 1 in 4 samples, which lands around a
2.5x zstd compression ratio. Adjust GATE_MASK (noise on 1 in
(GATE_MASK + 1) samples), NOISE_BITS, or TRI_PERIOD to tune the ratio.
"""

import hashlib
import random
import sys

BLOB_SIZE = 16 * 1024 * 1024
NUM_SAMPLES = BLOB_SIZE // 2
TRI_PERIOD = 4096  # samples per triangle-wave cycle
NOISE_BITS = 8  # noise amplitude: 0..(2^NOISE_BITS - 1)
GATE_MASK = 0x03  # noise hits samples where gate byte & GATE_MASK == 0
SEED = 1337


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <output_file>", file=sys.stderr)
        return 1

    # Triangle wave lookup table: rises 0..2048, falls back to 0, scaled
    # so samples span 0..8192 (comfortably within 16 bits after noise).
    half = TRI_PERIOD // 2
    tri = [(k if k < half else TRI_PERIOD - k) * 4 for k in range(TRI_PERIOD)]

    # random.Random is a seeded Mersenne Twister; CPython guarantees the
    # same seed yields the same sequence across versions and platforms.
    rng = random.Random(SEED)
    noise = rng.randbytes(NUM_SAMPLES)
    gate = rng.randbytes(NUM_SAMPLES)
    noise_mask = (1 << NOISE_BITS) - 1

    out = bytearray(BLOB_SIZE)
    for k in range(NUM_SAMPLES):
        s = tri[k % TRI_PERIOD]
        if gate[k] & GATE_MASK == 0:
            s += noise[k] & noise_mask
        out[2 * k] = s & 0xFF
        out[2 * k + 1] = (s >> 8) & 0xFF

    with open(sys.argv[1], "wb") as f:
        f.write(out)

    digest = hashlib.sha256(out).hexdigest()
    print(f"wrote {sys.argv[1]}: {BLOB_SIZE} bytes, sha256 {digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

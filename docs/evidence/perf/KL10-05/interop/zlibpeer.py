# librdkafka's gzip codec: deflateInit2(level, Z_DEFLATED, 15+16, memLevel 8, Z_DEFAULT_STRATEGY); inflate with 15+32.
import glob, os, sys, zlib
d = sys.argv[1]; fails = 0
for p in sorted(glob.glob(os.path.join(d, "rust-*.gz"))):
    name = os.path.basename(p)[:-3]; n = name.rsplit("-", 1)[1]
    want = open(os.path.join(d, f"section-{n}.bin"), "rb").read()
    got = zlib.decompressobj(15 + 32).decompress(open(p, "rb").read())
    ok = got == want; fails += not ok
    print(f"zlib {zlib.ZLIB_RUNTIME_VERSION} decodes {name}: {'ok' if ok else 'MISMATCH'}")
for n in (1, 16, 256):
    sec = open(os.path.join(d, f"section-{n}.bin"), "rb").read()
    for lvl in (-1, 1, 9):
        c = zlib.compressobj(lvl, zlib.DEFLATED, 15 + 16, 8, zlib.Z_DEFAULT_STRATEGY)
        open(os.path.join(d, f"zlib-l{'default' if lvl < 0 else lvl}-{n}.gz"), "wb").write(c.compress(sec) + c.flush())
print("zlib wrote sections"); sys.exit(1 if fails else 0)

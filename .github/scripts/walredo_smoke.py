#!/usr/bin/env python3
"""Smoke test for the `postgres --wal-redo` helper.

Drives the binary protocol over stdin/stdout without needing a cluster:
  - BEGIN + PUSHBASE(page) + GET round-trips the page with pd_lsn stamped (4a);
  - APPLY rejects malformed records (too short / length mismatch / bad CRC),
    exercising the validation that guards the decode path.

Usage: walredo_smoke.py <path-to-postgres-binary>
Exit status is nonzero if any check fails.
"""
import struct
import subprocess
import sys

PG = sys.argv[1]
BLCKSZ = 8192
SizeOfXLogRecord = 24            # 64-bit layout
RM_XLOG_ID = 0                   # a valid resource-manager id

fails = 0


def check(name, ok):
    global fails
    print(("PASS" if ok else "FAIL"), "-", name)
    if not ok:
        fails += 1


def run(payload):
    return subprocess.run([PG, "--wal-redo"], input=payload,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=60)


def begin():
    return b'b' + struct.pack('<5I', 1663, 5, 16384, 0, 0)


# 1) BEGIN + PUSHBASE(page) + GET: the page comes back with pd_lsn stamped.
page = bytearray(BLCKSZ)
struct.pack_into('<H', page, 14, 8000)        # pd_upper != 0 -> not a new page
marker = b'HELLO-WALREDO'
page[100:100 + len(marker)] = marker
base_lsn = 0x12345678ABCD
out = run(begin() + b'p' + struct.pack('<QI', base_lsn, BLCKSZ) + bytes(page) + b'g').stdout
check("GET returns BLCKSZ bytes", len(out) == BLCKSZ)
check("GET preserves page body", out[100:100 + len(marker)] == marker)
xlogid, xrecoff = struct.unpack('<II', out[0:8]) if len(out) >= 8 else (0, 0)
check("GET stamps pd_lsn = base_end_lsn", ((xlogid << 32) | xrecoff) == base_lsn)

# 2) APPLY with a record shorter than the header is rejected.
check("APPLY too-short record rejected (exit 1)",
      run(begin() + b'a' + struct.pack('<QQI', 0, 0, 8) + b'\0' * 8).returncode == 1)

# 3) APPLY whose header xl_tot_len disagrees with the framed length is rejected.
rec = bytearray(SizeOfXLogRecord + 8)
struct.pack_into('<I', rec, 0, 999)           # xl_tot_len != len
check("APPLY xl_tot_len mismatch rejected (exit 1)",
      run(begin() + b'a' + struct.pack('<QQI', 0, 0, len(rec)) + bytes(rec)).returncode == 1)

# 4) APPLY with matching length and valid rmid but a bad CRC is rejected.
rec = bytearray(SizeOfXLogRecord + 8)
struct.pack_into('<I', rec, 0, len(rec))      # xl_tot_len == len
rec[17] = RM_XLOG_ID                          # xl_rmid valid; xl_crc (off 20) left 0 -> mismatch
check("APPLY bad CRC rejected (exit 1)",
      run(begin() + b'a' + struct.pack('<QQI', 0, 0, len(rec)) + bytes(rec)).returncode == 1)

print("---", "all passed" if not fails else f"{fails} FAILED")
sys.exit(1 if fails else 0)

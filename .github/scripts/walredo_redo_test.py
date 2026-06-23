#!/usr/bin/env python3
"""End-to-end redo test for the `postgres --wal-redo` helper.

Generates a real WAL delta record, replays it onto a base page through the
helper, and checks the result matches the page the running server produced.

It uses a throwaway cluster:
  - insert a row, CHECKPOINT (so the next change to the page logs an FPI),
  - insert a second row (record A: carries the block's full-page image),
  - capture block 0 as the base,
  - insert a third row (record B: a pure delta, no FPI -- not the first change
    after the checkpoint),
  - CHECKPOINT and capture block 0 as the expected result.
Then BEGIN(target=block 0) + PUSHBASE(base) + APPLY(record B) + GET must equal
the expected page, except pd_lsn (offset 0..7, stamped by redo) and pd_checksum
(offset 8..9, which the helper deliberately leaves for the caller to recompute).

Usage: walredo_redo_test.py <bindir> <datadir> <port>
"""
import glob
import os
import re
import struct
import subprocess
import sys
import time

BINDIR, PGDATA, PORT = sys.argv[1], sys.argv[2], sys.argv[3]
os.environ["LC_ALL"] = "C"
SEGSIZE = 16 * 1024 * 1024


def pg_ctl(*a):
    subprocess.run([f"{BINDIR}/pg_ctl", "-D", PGDATA] + list(a), capture_output=True)


def Q(sql):
    return subprocess.run([f"{BINDIR}/psql", "-h", "/tmp", "-p", PORT, "-U", "postgres",
                           "-d", "postgres", "-tA", "-c", sql],
                          capture_output=True, text=True).stdout.strip()


def lsn2int(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


try:
    os.remove(f"{PGDATA}/postmaster.pid")
except OSError:
    pass

pg_ctl("-o", f"-p {PORT} -k /tmp", "-l", f"{PGDATA}/redo_test.log", "start")
time.sleep(2)
Q("create extension if not exists pageinspect;")
Q("drop table if exists wrt; create table wrt(id int primary key, v text) "
  "with (autovacuum_enabled=off);")
Q("insert into wrt values (1,'aaaa');")
Q("checkpoint;")
Q("insert into wrt values (2,'bbbb');")                 # record A: FPI carrier
base_hex = Q("select encode(get_raw_page('wrt',0),'hex');")
base_lsn = lsn2int(Q("select lsn from page_header(get_raw_page('wrt',0));"))
start_lsn = lsn2int(Q("select pg_current_wal_lsn();"))
Q("insert into wrt values (3,'cccc');")                 # record B: the delta
end_lsn = lsn2int(Q("select pg_current_wal_lsn();"))
Q("checkpoint;")
exp_hex = Q("select encode(get_raw_page('wrt',0),'hex');")
db = int(Q("select oid from pg_database where datname='postgres';"))
rel = int(Q("select relfilenode from pg_class where relname='wrt';"))
dump = subprocess.run([f"{BINDIR}/pg_waldump", "-p", f"{PGDATA}/pg_wal",
                       "-s", "%X/%08X" % (start_lsn >> 32, start_lsn & 0xFFFFFFFF),
                       "-e", "%X/%08X" % (end_lsn >> 32, end_lsn & 0xFFFFFFFF)],
                      capture_output=True, text=True).stdout
pg_ctl("stop", "-m", "fast")

# locate record B: a heap INSERT with no full-page image
all_lsns = sorted({lsn2int(m.group(1))
                   for m in re.finditer(r"lsn: ([0-9A-F]+/[0-9A-F]+)", dump)})
rec_start = None
for ln in dump.splitlines():
    if "Heap" in ln and "INSERT" in ln and "FPW" not in ln and "lsn:" in ln:
        rec_start = lsn2int(re.search(r"lsn: ([0-9A-F]+/[0-9A-F]+)", ln).group(1))
        break
assert rec_start is not None, "no no-FPI heap INSERT record found"
rec_end = min([x for x in all_lsns if x > rec_start] + [end_lsn])
rec_len = rec_end - rec_start
off = rec_start % SEGSIZE
assert (off % 8192) + rec_len <= 8192, "record spans a WAL page boundary; rerun"
seg = "%08X%08X%08X" % (1, rec_start >> 32, (rec_start % (1 << 32)) // SEGSIZE)
rec = open(f"{PGDATA}/pg_wal/{seg}", "rb").read()[off:off + rec_len]

msg = b'b' + struct.pack('<5I', 1663, db, rel, 0, 0)
msg += b'p' + struct.pack('<QI', base_lsn, 8192) + bytes.fromhex(base_hex)
msg += b'a' + struct.pack('<QQI', rec_start, rec_end, rec_len) + rec
msg += b'g'
p = subprocess.run([f"{BINDIR}/postgres", "--wal-redo", "-D", PGDATA],
                   input=msg, capture_output=True, timeout=120)
got, exp = p.stdout, bytes.fromhex(exp_hex)

ok = (p.returncode == 0 and len(got) == 8192 and got[10:] == exp[10:])
print("PASS" if ok else "FAIL",
      "- redo of a heap INSERT delta reproduces the server's page",
      "(ignoring pd_lsn + pd_checksum)")
if not ok:
    print("rc=%d len=%d" % (p.returncode, len(got)))
    print("stderr:", p.stderr.decode("latin1")[-800:])
    if len(got) == 8192:
        print("first body diffs:", [i for i in range(10, 8192) if got[i] != exp[i]][:12])
sys.exit(0 if ok else 1)

#!/usr/bin/env python3
"""End-to-end redo tests for the `postgres --wal-redo` helper.

Each case generates a real WAL delta record on a throwaway cluster, replays it
onto a captured base page through the helper, and checks the result matches the
page the running server produced -- except pd_lsn (offset 0..7, stamped by redo)
and pd_checksum (offset 8..9, which the helper leaves for the caller).

Cases:
  insert  - a no-FPI heap INSERT delta (third insert after a checkpoint+FPI).
  update  - a heap UPDATE that clears the all-visible VM bit, exercising the
            visibilitymap_pin/clear redo path; run with full_page_writes=off so
            the record is a small delta rather than a full-page image.

Usage: walredo_redo_test.py <bindir> <datadir> <port>
"""
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


def fmt(x):
    return "%X/%08X" % (x >> 32, x & 0xFFFFFFFF)


def start(extra_opts=""):
    try:
        os.remove(f"{PGDATA}/postmaster.pid")
    except OSError:
        pass
    pg_ctl("-o", f"-p {PORT} -k /tmp {extra_opts}", "-l", f"{PGDATA}/redo_test.log", "start")
    time.sleep(2)


def extract_record(start_lsn, end_lsn, desc_match):
    """Return (rec_start, rec_end, bytes) for the first matching no-FPI record."""
    dump = subprocess.run([f"{BINDIR}/pg_waldump", "-p", f"{PGDATA}/pg_wal",
                           "-s", fmt(start_lsn), "-e", fmt(end_lsn)],
                          capture_output=True, text=True).stdout
    all_lsns = sorted({lsn2int(m.group(1))
                       for m in re.finditer(r"lsn: ([0-9A-F]+/[0-9A-F]+)", dump)})
    rec_start = None
    for ln in dump.splitlines():
        if desc_match(ln) and "FPW" not in ln and "lsn:" in ln:
            rec_start = lsn2int(re.search(r"lsn: ([0-9A-F]+/[0-9A-F]+)", ln).group(1))
            break
    assert rec_start is not None, "no matching no-FPI record found"
    rec_end = min([x for x in all_lsns if x > rec_start] + [end_lsn])
    off = rec_start % SEGSIZE
    assert (off % 8192) + (rec_end - rec_start) <= 8192, "record spans a WAL page; rerun"
    seg = "%08X%08X%08X" % (1, rec_start >> 32, (rec_start % (1 << 32)) // SEGSIZE)
    data = open(f"{PGDATA}/pg_wal/{seg}", "rb").read()[off:off + (rec_end - rec_start)]
    return rec_start, rec_end, data


def replay(db, rel, base_hex, base_lsn, rec_start, rec_end, data):
    msg = b'b' + struct.pack('<5I', 1663, db, rel, 0, 0)
    msg += b'p' + struct.pack('<QI', base_lsn, 8192) + bytes.fromhex(base_hex)
    msg += b'a' + struct.pack('<QQI', rec_start, rec_end, len(data)) + data
    msg += b'g'
    return subprocess.run([f"{BINDIR}/postgres", "--wal-redo", "-D", PGDATA],
                          input=msg, capture_output=True, timeout=120)


def check(name, p, exp_hex):
    exp, got = bytes.fromhex(exp_hex), p.stdout
    ok = (p.returncode == 0 and len(got) == 8192 and got[10:] == exp[10:])
    print("PASS" if ok else "FAIL", "-", name)
    if not ok:
        print("  rc=%d len=%d" % (p.returncode, len(got)))
        print("  stderr:", p.stderr.decode("latin1")[-600:])
        if len(got) == 8192:
            print("  body diffs:", [i for i in range(10, 8192) if got[i] != exp[i]][:12])
    return ok


fails = 0

# --- case: no-FPI heap INSERT delta ---------------------------------------
start()
Q("create extension if not exists pageinspect;")
Q("drop table if exists wrt; create table wrt(id int primary key, v text) "
  "with (autovacuum_enabled=off);")
Q("insert into wrt values (1,'aaaa');")
Q("checkpoint;")
Q("insert into wrt values (2,'bbbb');")                 # record A: FPI carrier
base_hex = Q("select encode(get_raw_page('wrt',0),'hex');")
base_lsn = lsn2int(Q("select lsn from page_header(get_raw_page('wrt',0));"))
s = lsn2int(Q("select pg_current_wal_lsn();"))
Q("insert into wrt values (3,'cccc');")                 # record B: the delta
e = lsn2int(Q("select pg_current_wal_lsn();"))
Q("checkpoint;")
exp_hex = Q("select encode(get_raw_page('wrt',0),'hex');")
db = int(Q("select oid from pg_database where datname='postgres';"))
rel = int(Q("select relfilenode from pg_class where relname='wrt';"))
pg_ctl("stop", "-m", "fast")
rs, re_, data = extract_record(s, e, lambda ln: "Heap" in ln and "INSERT" in ln)
fails += not check("heap INSERT delta", replay(db, rel, base_hex, base_lsn, rs, re_, data), exp_hex)

# --- case: VM-clearing heap UPDATE delta (full_page_writes off) -----------
start("-c full_page_writes=off")
Q("drop table if exists vmt; create table vmt(id int primary key, v text) "
  "with (autovacuum_enabled=off);")
Q("insert into vmt values (1,'aaaa');")
Q("vacuum (freeze) vmt;")                               # set the all-visible VM bit
Q("checkpoint;")
base_hex = Q("select encode(get_raw_page('vmt',0),'hex');")
base_lsn = lsn2int(Q("select lsn from page_header(get_raw_page('vmt',0));"))
s = lsn2int(Q("select pg_current_wal_lsn();"))
Q("update vmt set v='bbbb' where id=1;")                # clears the VM bit
e = lsn2int(Q("select pg_current_wal_lsn();"))
Q("checkpoint;")
exp_hex = Q("select encode(get_raw_page('vmt',0),'hex');")
db = int(Q("select oid from pg_database where datname='postgres';"))
rel = int(Q("select relfilenode from pg_class where relname='vmt';"))
pg_ctl("stop", "-m", "fast")
rs, re_, data = extract_record(s, e, lambda ln: "Heap" in ln and "UPDATE" in ln)
fails += not check("VM-clearing heap UPDATE delta", replay(db, rel, base_hex, base_lsn, rs, re_, data), exp_hex)

sys.exit(1 if fails else 0)

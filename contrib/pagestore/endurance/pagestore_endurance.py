#!/usr/bin/env python3
"""Continuously run a real-PostgreSQL pagestore topology and look for bugs.

See ENDURANCE.md next to this file for the design, the oracles and the known
gaps.  Standard library only; pass the meson build directory (with a
tmp_install) as --build.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


class Failure(Exception):
    """A product-level finding: the run stops and its root is preserved."""

    def __init__(self, kind: str, detail: str):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


# Log lines that are never legitimate, whatever event the driver injected.
# "terminated by signal 9" is deliberately absent: the driver sends SIGKILL.
BAD_LOG = re.compile(
    r"PANIC:|TRAP:|invalid page in block|could not read block|"
    r"unexpected data beyond EOF|terminated by signal (6|7|11)\b|"
    r"AddressSanitizer|stack smashing"
)

NACCTS_PER_SCALE = 20000
NDOCS_PER_SCALE = 200
INITIAL_BALANCE = 1000

WORKLOAD = {
    # weight, script
    "transfer": (10, """\
\\set a random(1, :naccts)
\\set b random(1, :naccts)
\\set lo least(:a, :b)
\\set hi greatest(:a, :b)
\\set amt random(1, 100)
BEGIN;
UPDATE acct SET bal = bal - :amt WHERE id = :lo;
UPDATE acct SET bal = bal + :amt WHERE id = :hi;
END;
"""),
    "ev_insert": (8, """\
\\set k random(1, :naccts)
INSERT INTO ev(k, payload) VALUES (:k, repeat(md5(random()::text), (random() * 20)::int + 1));
"""),
    "ev_delete": (2, """\
\\set k random(1, :naccts)
DELETE FROM ev WHERE k BETWEEN :k AND :k + 5;
"""),
    "doc_update": (2, """\
\\set id random(1, :ndocs)
\\set n random(1, 3000)
UPDATE doc SET ver = ver + 1,
  body = (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, :n))
  WHERE id = :id;
"""),
    "abort": (2, """\
\\set a random(1, :naccts)
BEGIN;
UPDATE acct SET bal = bal + 12345 WHERE id = :a;
INSERT INTO ev(k, payload) VALUES (:a, 'aborted');
ROLLBACK;
"""),
    "savepoint": (2, """\
\\set a random(1, :naccts)
BEGIN;
SAVEPOINT s;
UPDATE acct SET bal = bal + 777 WHERE id = :a;
ROLLBACK TO s;
INSERT INTO ev(k, payload) VALUES (:a, 'after-subabort');
COMMIT;
"""),
    "twophase": (1, """\
\\set a random(1, :naccts)
\\set b random(1, :naccts)
\\set lo least(:a, :b)
\\set hi greatest(:a, :b)
\\set r random(1, 1000000000)
BEGIN;
UPDATE acct SET bal = bal - 1 WHERE id = :lo;
UPDATE acct SET bal = bal + 1 WHERE id = :hi;
PREPARE TRANSACTION 'e:client_id-:r';
COMMIT PREPARED 'e:client_id-:r';
"""),
    "lock_share": (1, """\
\\set a random(1, :naccts)
BEGIN;
SELECT id FROM acct WHERE id BETWEEN :a AND :a + 3 FOR KEY SHARE;
SELECT pg_sleep(0.01);
COMMIT;
"""),
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Pg:
    """One PostgreSQL compute owned by the driver."""

    def __init__(self, run: "Run", name: str, datadir: Path):
        self.run = run
        self.name = name
        self.datadir = datadir
        self.port = free_port()
        self.log = run.root / f"{name}.log"
        self.expected_up = False
        self.log_scanned = 0

    def ctl(self, *args: str, env: dict[str, str] | None = None, timeout: int = 300) -> subprocess.CompletedProcess:
        return subprocess.run(
            [str(self.run.bin / "pg_ctl"), "-D", str(self.datadir), *args],
            env=env or self.run.env, capture_output=True, text=True, timeout=timeout)

    def start(self, tries: int = 1) -> None:
        for attempt in range(tries):
            r = self.ctl("-l", str(self.log), "-w", "-t", "300", "start")
            if r.returncode == 0:
                self.expected_up = True
                return
            time.sleep(1)
        raise Failure("start_failed", f"{self.name} did not start: {r.stdout[-400:]} {r.stderr[-400:]}")

    def stop(self, mode: str = "fast", check: bool = True) -> None:
        self.expected_up = False
        r = self.ctl("-m", mode, "-w", "-t", "300", "stop")
        if check and r.returncode != 0:
            raise Failure("stop_failed", f"{self.name} ({mode}): {r.stdout[-300:]} {r.stderr[-300:]}")

    def pid(self) -> int | None:
        try:
            return int((self.datadir / "postmaster.pid").read_text().splitlines()[0])
        except (OSError, ValueError, IndexError):
            return None

    def sql(self, sql: str, timeout: int = 600, check: bool = True) -> str:
        r = subprocess.run(
            [str(self.run.bin / "psql"), "-X", "-h", "127.0.0.1", "-p", str(self.port),
             "-U", "postgres", "-d", "postgres", "-qtA", "-v", "ON_ERROR_STOP=1", "-c", sql],
            env=self.run.env, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            if check:
                raise SqlError(self.name, sql, r.stderr.strip())
            return ""
        return r.stdout.strip()

    def scan_log(self) -> None:
        try:
            with open(self.log, "r", errors="replace") as f:
                f.seek(self.log_scanned)
                chunk = f.read()
                self.log_scanned = f.tell()
        except OSError:
            return
        for line in chunk.splitlines():
            if BAD_LOG.search(line):
                raise Failure("bad_log", f"{self.name}: {line[:400]}")


class SqlError(Exception):
    def __init__(self, target: str, sql: str, stderr: str):
        super().__init__(f"{target}: {stderr[:400]} [{sql[:120]}]")
        self.stderr = stderr


class Run:
    def __init__(self, args: argparse.Namespace, seed: int, root: Path):
        self.args = args
        self.seed = seed
        self.rng = random.Random(seed)
        self.root = root
        build = Path(args.build).resolve()
        pgctl = next(build.glob("tmp_install/**/bin/pg_ctl"), None)
        if pgctl is None:
            sys.exit(f"no tmp_install under {build} (run: meson test -C {build} --suite setup)")
        self.bin = pgctl.parent
        prefix = self.bin.parent
        self.env = dict(os.environ, LD_LIBRARY_PATH=f"{prefix}/lib:{prefix}/lib64", PGCONNECT_TIMEOUT="20")
        for k in [k for k in self.env if k.startswith("PAGESTORE_TEST_FAULT")]:
            del self.env[k]
        cb = build / "contrib/pagestore"
        self.daemon_bin, self.import_bin = cb / "pagestore_daemon", cb / "pagestore_import"
        self.inspect_bin, self.walrestore = cb / "pagestore_inspect", cb / "pagestore_walrestore"
        self.control_restore = cb / "pagestore_control_restore"
        self.branch_prepare = self.bin / "pagestore_branch_prepare"
        self.store = root / "store"
        self.shm = f"/psendur_{os.getpid()}_{seed % 100000}"
        self.daemon: subprocess.Popen | None = None
        self.daemon_expected_up = False
        self.writer = Pg(self, "writer", root / "writer")
        self.mat = Pg(self, "materializer", root / "materializer")
        self.branches: list[tuple[int, Pg, Path]] = []   # (timeline, compute, scratch)
        self.next_timeline = 1
        self.round = 0
        self.naccts = NACCTS_PER_SCALE * args.scale
        self.ndocs = NDOCS_PER_SCALE * args.scale
        self.events = open(root / "events.jsonl", "a", buffering=1)
        self.stop_sampler = threading.Event()
        self.async_failure: Failure | None = None
        self.stats = {"rounds": 0, "verifies": 0, "branches": 0, "events": {}}

    # ---- bookkeeping ----------------------------------------------------

    def event(self, kind: str, **fields: Any) -> None:
        rec = {"t": round(time.time(), 3), "round": self.round, "event": kind, **fields}
        self.events.write(json.dumps(rec) + "\n")
        if kind not in ("sample",):
            print(f"[{time.strftime('%H:%M:%S')}] seed={self.seed} r={self.round} {kind} "
                  + " ".join(f"{k}={v}" for k, v in fields.items())[:300], flush=True)
        self.stats["events"][kind] = self.stats["events"].get(kind, 0) + 1

    def computes(self) -> list[Pg]:
        return [self.writer, self.mat] + [b[1] for b in self.branches]

    def check_health(self) -> None:
        if self.async_failure:
            raise self.async_failure
        if self.daemon_expected_up and self.daemon and self.daemon.poll() is not None:
            raise Failure("daemon_died", f"pagestore_daemon exited with {self.daemon.returncode}")
        for pg in self.computes():
            pg.scan_log()
            if pg.expected_up:
                pid = pg.pid()
                if pid is None or not Path(f"/proc/{pid}").exists():
                    raise Failure("compute_died", f"{pg.name} postmaster is gone")

    # ---- daemon ---------------------------------------------------------

    def start_daemon(self) -> None:
        log = open(self.root / "daemon.log", "a")
        self.daemon = subprocess.Popen(
            [str(self.daemon_bin), "--shm", self.shm, "--store", str(self.store), *self.args.daemon_arg],
            stdout=log, stderr=subprocess.STDOUT, env=self.env)
        deadline = time.time() + 300
        while time.time() < deadline:
            if self.daemon.poll() is not None:
                raise Failure("daemon_start", f"daemon exited with {self.daemon.returncode} during recovery")
            if subprocess.run([str(self.inspect_bin), "--shm", self.shm, "health"],
                              capture_output=True, env=self.env).returncode == 0:
                self.daemon_expected_up = True
                return
            time.sleep(0.1)
        raise Failure("daemon_start", "daemon did not become ready in 300 s")

    def stop_daemon(self, sig: int = signal.SIGTERM) -> None:
        self.daemon_expected_up = False
        if not self.daemon:
            return
        self.daemon.send_signal(sig)
        try:
            rc = self.daemon.wait(timeout=600)
        except subprocess.TimeoutExpired:
            raise Failure("daemon_hang", f"daemon ignored signal {sig} for 600 s")
        if sig == signal.SIGTERM and rc != 0:
            raise Failure("daemon_exit", f"clean daemon shutdown returned {rc}")
        self.daemon = None

    def inspect(self, what: str) -> str:
        r = subprocess.run([str(self.inspect_bin), "--shm", self.shm, what],
                           capture_output=True, text=True, env=self.env, timeout=60)
        return r.stdout.strip()

    # ---- provisioning (mirrors mvp_golden_test.sh) ------------------------

    def run_tool(self, argv: list[str], what: str, timeout: int = 900) -> str:
        r = subprocess.run(argv, env=self.env, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            raise Failure("tool_failed", f"{what}: rc={r.returncode} {r.stderr[-1500:]}")
        return r.stdout

    def wal_segment_size(self, datadir: Path) -> int:
        out = self.run_tool([str(self.bin / "pg_controldata"), str(datadir)], "pg_controldata")
        return int(re.search(r"Bytes per WAL segment:\s*(\d+)", out).group(1))

    def provision(self) -> None:
        self.store.mkdir(parents=True)
        self.run_tool([str(self.bin / "initdb"), "-D", str(self.writer.datadir), "-U", "postgres",
                       "-A", "trust"], "writer initdb")
        self.start_daemon()
        self.run_tool([str(self.import_bin), "--shm", self.shm, "--pgdata", str(self.writer.datadir)],
                      "pagestore_import")
        with open(self.writer.datadir / "postgresql.conf", "a") as f:
            f.write(f"""
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '{self.shm}'
pagestore.route_all = off
pagestore.timeline = 0
io_method = sync
recovery_prefetch = try
archive_mode = on
archive_library = 'pagestore'
listen_addresses = '127.0.0.1'
port = {self.writer.port}
track_commit_timestamp = on
max_prepared_transactions = 64
max_connections = 100
checkpoint_timeout = 30s
max_wal_size = 128MB
autovacuum_naptime = 5s
log_line_prefix = '%m [%p] '
""")
        self.writer.start()
        self.run_tool([str(self.bin / "pg_basebackup"), "-h", "127.0.0.1", "-p", str(self.writer.port),
                       "-U", "postgres", "-D", str(self.mat.datadir), "--wal-method=none",
                       "--checkpoint=fast"], "materializer base backup")
        segsize = self.wal_segment_size(self.mat.datadir)
        self.archive_current_wal()
        with open(self.mat.datadir / "postgresql.conf", "a") as f:
            f.write(f"""
pagestore.route_all = on
pagestore.materializer = on
pagestore.retention_owner_id = '1'
pagestore.retention_owner_generation = '1'
archive_mode = off
port = {self.mat.port}
hot_standby = on
shared_buffers = 32MB
max_standby_archive_delay = 60s
restore_command = '{self.walrestore} --shm {self.shm} --timeline 0 --incarnation 1 --segsize {segsize} %f %p'
""")
        (self.mat.datadir / "standby.signal").touch()
        for seg in (self.mat.datadir / "pg_wal").glob("0000000*"):
            if seg.is_file():
                seg.unlink()
        self.mat.start()
        self.writer.sql("CREATE SCHEMA pagestore_ext; CREATE EXTENSION pagestore WITH SCHEMA pagestore_ext;"
                        " CREATE EXTENSION amcheck;")
        self.writer.sql(f"""
CREATE TABLE acct(id int PRIMARY KEY, bal bigint NOT NULL, filler text);
INSERT INTO acct SELECT g, {INITIAL_BALANCE}, repeat('x', 80) FROM generate_series(1, {self.naccts}) g;
CREATE TABLE doc(id int PRIMARY KEY, ver int NOT NULL DEFAULT 0, body text);
ALTER TABLE doc ALTER COLUMN body SET STORAGE EXTERNAL;
INSERT INTO doc SELECT g, 0, repeat(md5(g::text), 100) FROM generate_series(1, {self.ndocs}) g;
CREATE TABLE ev(id bigserial PRIMARY KEY, k int NOT NULL REFERENCES acct(id), payload text,
                ts timestamptz DEFAULT now());
CREATE INDEX ev_k ON ev(k);
""", timeout=1800)
        # retention-owner authority for the branch controller
        auth = self.root / "controller-authority"
        auth.mkdir(mode=0o700)
        ms, as_ = self.mat.datadir.stat(), auth.stat()
        (auth / "retention-owner-1.json").write_text(json.dumps({
            "retention_generation": 1, "consumer_data_dir": str(self.mat.datadir),
            "consumer_instance_id": "endurance", "consumer_data_dev": ms.st_dev,
            "consumer_data_ino": ms.st_ino, "authority_namespace_dev": as_.st_dev,
            "authority_namespace_ino": as_.st_ino}) + "\n")
        wl = self.root / "workload"
        wl.mkdir()
        for name, (_, body) in WORKLOAD.items():
            (wl / f"{name}.sql").write_text(body)

    # ---- WAL shipping and materialization waits --------------------------

    def wait_for(self, what: str, fn, timeout: float) -> None:
        deadline = time.time() + timeout
        last: Any = None
        while time.time() < deadline:
            self.check_health()
            try:
                last = fn()
                if last is True:
                    return
            except (SqlError, subprocess.TimeoutExpired) as e:
                last = str(e)
            time.sleep(0.2)
        self.dump_diagnostics()
        raise Failure("timeout", f"{what} not reached in {timeout:.0f} s (last: {str(last)[:300]})")

    def archive_current_wal(self) -> str:
        switch = self.writer.sql("SELECT pg_switch_wal();")
        seg = self.writer.sql(f"SELECT pg_walfile_name('{switch}'::pg_lsn - 1);")
        done = self.writer.datadir / "pg_wal/archive_status" / f"{seg}.done"
        self.wait_for(f"archive of {seg}", lambda: done.exists() or not
                      (self.writer.datadir / "pg_wal" / seg).exists(), self.args.sync_timeout)
        return switch

    def sync_materializer(self) -> str:
        t0 = time.time()
        lsn = self.archive_current_wal()
        self.timing = {"archive_s": round(time.time() - t0, 1)}
        t0 = time.time()
        self.wait_for(f"materializer replay through {lsn}", lambda: self.mat.sql(
            f"SELECT pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn;") == "t", self.args.sync_timeout)
        self.timing["replay_s"] = round(time.time() - t0, 1)
        return lsn

    # ---- oracles ---------------------------------------------------------

    def tables(self, pg: Pg) -> list[str]:
        out = pg.sql("SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                     "WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY 1;")
        return out.split("\n") if out else []

    def query_retry(self, pg: Pg, sql: str) -> str:
        for attempt in range(5):
            try:
                return pg.sql(sql, timeout=3600)
            except SqlError as e:
                if "conflict with recovery" in e.stderr and attempt < 4:
                    time.sleep(1)
                    continue
                raise Failure("query_failed", str(e))
        raise AssertionError

    def checksums(self, pg: Pg) -> dict[str, str]:
        result = {}
        for t in self.tables(pg):
            result[t] = self.query_retry(
                pg, f'SELECT count(*)::text || \':\' || coalesce(sum(hashtextextended(t::text, 0)::numeric), 0)::text '
                    f'FROM public."{t}" t;')
        return result

    def check_invariants(self, pg: Pg, amcheck: bool) -> None:
        total = self.query_retry(pg, "SELECT sum(bal) FROM acct;")
        if total != str(self.naccts * INITIAL_BALANCE):
            raise Failure("invariant", f"{pg.name}: sum(bal)={total}, expected {self.naccts * INITIAL_BALANCE}")
        # index path versus heap path for the same predicate
        k = self.rng.randint(1, self.naccts)
        a = self.query_retry(pg, f"SET enable_seqscan = off; SELECT count(*), coalesce(sum(id), 0) FROM ev WHERE k = {k};")
        b = self.query_retry(pg, f"SET enable_indexscan = off; SET enable_bitmapscan = off; "
                                 f"SELECT count(*), coalesce(sum(id), 0) FROM ev WHERE k = {k};")
        if a != b:
            raise Failure("index_heap_mismatch", f"{pg.name}: ev.k={k} index={a} heap={b}")
        if amcheck:
            bad = self.query_retry(pg, """
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT c.oid::regclass AS idx FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_am a ON a.oid = c.relam
           WHERE n.nspname = 'public' AND a.amname = 'btree' AND i.indisvalid LOOP
    PERFORM bt_index_check(r.idx, true);
  END LOOP; END $$;""")
            if bad:
                raise Failure("amcheck", f"{pg.name}: {bad[:400]}")

    def verify_materializer(self, why: str) -> dict[str, str]:
        """The writer keeps its heap local (route_all=off); the materializer
        serves the same relations from the store after replaying shipped WAL.
        With the writer quiescent they must agree."""
        self.resolve_prepared(self.writer)
        lsn = self.sync_materializer()
        amcheck = self.rng.random() < 0.34
        t0 = time.time()
        want, got = self.checksums(self.writer), self.checksums(self.mat)
        self.timing["checksum_s"] = round(time.time() - t0, 1)
        if want != got:
            diff = {t: (want.get(t), got.get(t)) for t in set(want) | set(got) if want.get(t) != got.get(t)}
            raise Failure("content_mismatch", f"materializer != writer at {lsn} ({why}): {diff}")
        self.check_invariants(self.writer, amcheck)
        self.check_invariants(self.mat, amcheck)
        self.stats["verifies"] += 1
        self.event("verified", why=why, lsn=lsn, tables=len(want), amcheck=amcheck, **self.timing)
        return want

    def resolve_prepared(self, pg: Pg) -> None:
        gids = pg.sql("SELECT gid FROM pg_prepared_xacts;")
        for gid in filter(None, gids.split("\n")):
            pg.sql(f"ROLLBACK PREPARED '{gid}';")
        if gids:
            self.event("resolved_prepared", target=pg.name, n=len(gids.split("\n")))

    # ---- workload --------------------------------------------------------

    def pgbench(self, pg: Pg, seconds: int, clients: int) -> subprocess.Popen:
        argv = [str(self.bin / "pgbench"), "-n", "-h", "127.0.0.1", "-p", str(pg.port), "-U", "postgres",
                "-c", str(clients), "-j", str(min(clients, 4)), "-T", str(seconds),
                f"--random-seed={self.rng.randint(1, 2**31 - 1)}",
                "-D", f"naccts={self.naccts}", "-D", f"ndocs={self.ndocs}"]
        for name, (weight, _) in WORKLOAD.items():
            argv += ["-f", f"{self.root / 'workload' / (name + '.sql')}@{weight}"]
        argv.append("postgres")
        return subprocess.Popen(argv, env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def ddl_chaos(self, stop: threading.Event, tolerate: threading.Event, rng: random.Random) -> None:
        n = 0
        while not stop.wait(rng.uniform(0.3, 2.0)):
            try:
                scratch = [t for t in self.tables(self.writer) if t.startswith("scratch_")]
                ops = ["create", "vacuum", "checkpoint", "analyze"]
                if scratch:
                    ops += ["drop", "truncate", "vacuum_full", "add_column", "reindex", "cluster", "churn", "churn"]
                if len(scratch) >= 6:
                    ops.remove("create")
                op = rng.choice(ops)
                t = rng.choice(scratch) if scratch else None
                if op == "create":
                    n += 1
                    name = f"scratch_{self.round}_{n}"
                    rows = rng.choice([10, 1000, 50000])
                    sql = (f"CREATE TABLE {name} AS SELECT g AS id, md5(g::text) AS v FROM generate_series(1, {rows}) g; "
                           f"ALTER TABLE {name} ADD PRIMARY KEY (id); CREATE INDEX ON {name}(v);")
                elif op == "drop":
                    sql = f"DROP TABLE {t};"
                elif op == "truncate":
                    sql = f"TRUNCATE {t};"
                elif op == "vacuum_full":
                    sql = f"VACUUM FULL {t};"
                elif op == "add_column":
                    sql = f"ALTER TABLE {t} ADD COLUMN c{rng.randint(1, 10**6)} int DEFAULT {rng.randint(1, 9)};"
                elif op == "reindex":
                    sql = f"REINDEX TABLE {t};"
                elif op == "cluster":
                    sql = f"CLUSTER {t} USING {t}_pkey;"
                elif op == "churn":
                    base = rng.randint(10**6, 10**8)
                    sql = (f"INSERT INTO {t}(id, v) SELECT g, md5(g::text) FROM generate_series({base}, {base + 5000}) g "
                           f"ON CONFLICT DO NOTHING; DELETE FROM {t} WHERE id % {rng.randint(2, 7)} = 0;")
                elif op == "vacuum":
                    sql = rng.choice(["VACUUM acct;", "VACUUM ev;", "VACUUM doc;", "VACUUM (FREEZE) ev;", "VACUUM;"])
                elif op == "analyze":
                    sql = "ANALYZE;"
                else:
                    sql = "CHECKPOINT;"
                self.writer.sql(sql, timeout=900)
            except (SqlError, subprocess.TimeoutExpired) as e:
                if not tolerate.is_set():
                    self.async_failure = Failure("ddl_failed", str(e))
                    return
                time.sleep(1)

    def burst(self) -> None:
        """One seeded window of concurrent SQL, DDL and one injected event."""
        seconds = self.rng.randint(self.args.burst_min, self.args.burst_max)
        event = self.rng.choices(
            ["none", "mat_restart", "mat_immediate", "mat_kill9", "mat_checkpoint",
             "writer_immediate", "writer_backend_kill9"],
            [4, 2, 2, 1, 2, 1, 1])[0]
        self.event("burst", seconds=seconds, inject=event)
        stop, tolerate = threading.Event(), threading.Event()
        chaos = threading.Thread(target=self.ddl_chaos, args=(stop, tolerate, random.Random(self.rng.random())))
        bench = self.pgbench(self.writer, seconds, self.args.clients)
        chaos.start()
        try:
            time.sleep(seconds * self.rng.uniform(0.2, 0.8))
            self.check_health()
            if event == "mat_restart":
                self.mat.stop("fast"); self.mat.start()
            elif event == "mat_immediate":
                self.mat.stop("immediate"); self.mat.start()
            elif event == "mat_kill9":
                self.mat.expected_up = False
                os.kill(self.mat.pid(), signal.SIGKILL)
                time.sleep(2)
                self.mat.start(tries=60)
            elif event == "mat_checkpoint":
                self.mat.sql("CHECKPOINT;", check=False)
            elif event == "writer_immediate":
                tolerate.set()
                self.writer.stop("immediate"); self.writer.start()
            elif event == "writer_backend_kill9":
                tolerate.set()
                pid = self.writer.sql("SELECT pid FROM pg_stat_activity WHERE backend_type = 'client backend' "
                                      "AND pid <> pg_backend_pid() ORDER BY random() LIMIT 1;", check=False)
                if pid:
                    os.kill(int(pid), signal.SIGKILL)
            out, _ = bench.communicate(timeout=seconds + 900)
        finally:
            stop.set()
            chaos.join()
            if bench.poll() is None:
                bench.kill()
        if bench.returncode != 0 and not tolerate.is_set():
            raise Failure("workload_failed", f"pgbench rc={bench.returncode}: {out[-1500:]}")
        if tolerate.is_set():
            # crash recovery may still be running after a backend SIGKILL
            self.wait_for("writer accepts connections after the injected crash",
                          lambda: self.writer.sql("SELECT 1;") == "1", 600)
        m = re.search(r"number of transactions actually processed: (\d+)", out or "")
        self.event("burst_done", tx=int(m.group(1)) if m else None, rc=bench.returncode)

    # ---- store restart ---------------------------------------------------

    def restart_store(self, crash: bool) -> None:
        """The MVP contract: every attached client stops before the daemon does."""
        self.event("store_restart", crash=crash)
        for _, b, _ in self.branches:
            b.stop("fast")
        self.mat.stop("fast")
        self.writer.stop("fast")
        self.stop_daemon(signal.SIGKILL if crash else signal.SIGTERM)
        self.start_daemon()
        self.writer.start()
        self.mat.start()
        for _, b, _ in self.branches:
            b.start()

    # ---- branches --------------------------------------------------------

    def create_branch(self, fork_state: dict[str, str]) -> None:
        tl = self.next_timeline
        self.next_timeline += 1
        prepared = self.root / f"prepared-{tl}"
        prepared.mkdir()
        psock = self.root / f"psock-{tl}"
        config = self.root / f"branch-{tl}.json"
        config.write_text(json.dumps({
            "schema": 2, "pg_ctl": str(self.bin / "pg_ctl"), "psql": str(self.bin / "psql"),
            "writer_data_dir": str(self.writer.datadir), "writer_host": "127.0.0.1",
            "writer_port": self.writer.port, "writer_log_file": str(self.writer.log),
            "private_socket_dir": str(psock), "private_port": free_port(),
            "materializer_data_dir": str(self.mat.datadir), "materializer_host": "127.0.0.1",
            "materializer_port": self.mat.port,
            "retention_authority_dir": str(self.root / "controller-authority"),
            "retention_owner_id": 1, "prepared_dir": str(prepared), "new_timeline": tl,
            "parent_timeline": 0, "database": "postgres", "user": "postgres",
            "poll_interval_ms": 50, "progress_timeout_ms": int(self.args.sync_timeout * 1000),
            "command_timeout_seconds": 600}, indent=1))
        receipt = None
        for attempt in range(4):
            # The controller publishes its own base checkpoint (finding E-1).
            # The fork capture can still lose the same race against the
            # materializer's own restartpoint; count how often it does.
            self.writer.expected_up = False      # the controller owns the writer for the window
            try:
                receipt = json.loads(self.run_tool(
                    [str(self.branch_prepare), "--config", str(config), "--verify-seed-against-materializer"],
                    f"branch controller for timeline {tl}", timeout=3600))
                break
            except Failure as f:
                if "did not durably cover the paused WAL replay position" not in f.detail or attempt == 3:
                    raise
                self.event("branch_prepare_retry", timeline=tl, attempt=attempt + 1)
            finally:
                self.writer.expected_up = True
        if receipt.get("state") != "complete":
            raise Failure("branch_prepare", f"receipt state {receipt.get('state')}")
        redo, ckpt, fork = receipt["checkpoint_redo_lsn"], receipt["checkpoint_end_lsn"], receipt["fork_lsn"]
        if self.checksums(self.writer) != fork_state:
            raise Failure("branch_window", "the writer's contents changed across the quiescent branch window")

        b = Pg(self, f"branch{tl}", self.root / f"branch-{tl}")
        scratch = self.root / f"branch-{tl}-walredo"
        self.run_tool([str(self.bin / "initdb"), "-D", str(b.datadir), "-U", "postgres", "-A", "trust"], "branch initdb")
        self.run_tool([str(self.control_restore), "--shm", self.shm, "--timeline", str(tl), "--incarnation", "1",
                       "--lsn", redo, "--archive-bootstrap", str(b.datadir)], "branch control restore")
        segsize = self.wal_segment_size(b.datadir)
        for seg in (b.datadir / "pg_wal").glob("0000000*"):
            if seg.is_file():
                seg.unlink()
        self.run_tool([str(self.bin / "initdb"), "-D", str(scratch), "-U", "postgres", "-A", "trust"], "walredo initdb")
        with open(b.datadir / "postgresql.conf", "a") as f:
            f.write(f"""
shared_preload_libraries = 'pagestore'
pagestore.backend = 'localsvc'
pagestore.localsvc_shm = '{self.shm}'
pagestore.route_all = on
pagestore.timeline = {tl}
pagestore.walredo_datadir = '{scratch}'
io_method = sync
archive_mode = off
listen_addresses = '127.0.0.1'
port = {b.port}
track_commit_timestamp = on
max_prepared_transactions = 64
max_connections = 100
shared_buffers = 32MB
checkpoint_timeout = 30s
autovacuum_naptime = 5s
log_line_prefix = '%m [%p] '
restore_command = '{self.walrestore} --shm {self.shm} --timeline {tl} --incarnation 1 --segsize {segsize} %f %p'
recovery_target_lsn = '{ckpt}'
recovery_target_inclusive = on
recovery_target_action = 'promote'
""")
        (b.datadir / "recovery.signal").touch()
        self.writer.sql(f"SELECT pagestore_ext.pagestore_install_prepared_branch_bootstrap("
                        f"'{prepared}', '{b.datadir}', {tl}, 0, '{redo}', '{ckpt}', '{fork}');")
        b.start()
        self.wait_for(f"branch {tl} promotion", lambda: b.sql("SELECT pg_is_in_recovery();") == "f", 600)
        got = self.checksums(b)
        if got != fork_state:
            diff = {t: (fork_state.get(t), got.get(t)) for t in set(fork_state) | set(got)
                    if fork_state.get(t) != got.get(t)}
            raise Failure("branch_mismatch", f"timeline {tl} at fork {fork} != writer's fork state: {diff}")
        self.check_invariants(b, amcheck=True)
        self.branches.append((tl, b, scratch))
        self.stats["branches"] += 1
        self.event("branch_created", timeline=tl, fork=fork, redo=redo)
        self.exercise_branch(tl, b)
        if self.checksums(self.writer) != fork_state:
            raise Failure("isolation", f"parent contents changed while only branch {tl} was written")
        while len(self.branches) > self.args.max_branches:
            old_tl, old, old_scratch = self.branches.pop(0)
            old.stop("fast")
            shutil.rmtree(old.datadir)
            shutil.rmtree(old_scratch)
            # GAP: no operator tool issues PS_OP_BEGIN_DELETE, so the store
            # keeps the retired timeline; see ENDURANCE.md.
            self.event("branch_retired", timeline=old_tl)

    def exercise_branch(self, tl: int, b: Pg) -> None:
        """Diverge the branch, then prove its own writes survive a restart with
        an empty buffer cache (every page comes back from the store)."""
        bench = self.pgbench(b, self.rng.randint(5, 15), max(2, self.args.clients // 2))
        out, _ = bench.communicate(timeout=900)
        if bench.returncode != 0:
            raise Failure("workload_failed", f"branch {tl} pgbench rc={bench.returncode}: {out[-1500:]}")
        self.resolve_prepared(b)
        b.sql("CHECKPOINT;")
        before = self.checksums(b)
        mode = self.rng.choice(["fast", "immediate"])
        b.stop(mode)
        b.start()
        after = self.checksums(b)
        if before != after:
            diff = {t: (before.get(t), after.get(t)) for t in set(before) | set(after) if before.get(t) != after.get(t)}
            raise Failure("branch_restart_mismatch", f"timeline {tl} changed across a {mode} restart: {diff}")
        self.check_invariants(b, amcheck=self.rng.random() < 0.5)
        self.event("branch_exercised", timeline=tl, restart=mode)

    # ---- sampling and diagnostics ---------------------------------------

    def sampler(self) -> None:
        out = open(self.root / "metrics.jsonl", "a", buffering=1)
        while not self.stop_sampler.wait(self.args.sample_interval):
            rec: dict[str, Any] = {"t": round(time.time(), 1), "round": self.round, "procs": {}}
            pids = {"daemon": self.daemon.pid if self.daemon else None}
            pids.update({pg.name: pg.pid() for pg in self.computes()})
            for name, pid in pids.items():
                try:
                    status = Path(f"/proc/{pid}/status").read_text()
                    rec["procs"][name] = {
                        "rss_kb": int(re.search(r"VmRSS:\s+(\d+)", status).group(1)),
                        "fds": len(os.listdir(f"/proc/{pid}/fd"))}
                except (OSError, AttributeError, TypeError):
                    pass
            try:
                du = subprocess.run(["du", "-sk", "--", *[str(p) for p in sorted(self.store.iterdir())]],
                                    capture_output=True, text=True, timeout=120).stdout
                rec["store_kb"] = {Path(l.split("\t")[1]).name: int(l.split("\t")[0]) for l in du.splitlines() if "\t" in l}
                total_gb = sum(rec["store_kb"].values()) / 1048576
                if total_gb > self.args.max_store_gb:
                    self.async_failure = Failure("store_size", f"store is {total_gb:.1f} GiB > --max-store-gb")
            except (OSError, subprocess.TimeoutExpired, ValueError):
                pass
            out.write(json.dumps(rec) + "\n")

    def dump_diagnostics(self) -> None:
        d = self.root / "diagnostics"
        d.mkdir(exist_ok=True)
        stamp = time.strftime("%H%M%S")
        for what in ("health", "backpressure", "gc", "owners", "timeline", "manifest"):
            try:
                (d / f"{stamp}-inspect-{what}.txt").write_text(self.inspect(what))
            except (OSError, subprocess.TimeoutExpired):
                pass
        for pg in self.computes():
            try:
                (d / f"{stamp}-{pg.name}-activity.txt").write_text(pg.sql(
                    "SELECT pid, backend_type, state, wait_event_type, wait_event, left(query, 200) "
                    "FROM pg_stat_activity;", timeout=20, check=False))
            except (OSError, subprocess.TimeoutExpired):
                pass

    def teardown(self, preserve_state: bool) -> None:
        self.stop_sampler.set()
        for pg in self.computes():
            try:
                pg.stop("immediate", check=False)
            except subprocess.TimeoutExpired:
                pass
        if self.daemon and self.daemon.poll() is None:
            # after a finding, SIGKILL keeps the store exactly as it failed
            self.daemon.send_signal(signal.SIGKILL if preserve_state else signal.SIGTERM)
            try:
                self.daemon.wait(timeout=600)
            except subprocess.TimeoutExpired:
                self.daemon.kill()
        try:
            os.unlink(f"/dev/shm{self.shm}")
        except OSError:
            pass
        self.events.close()

    # ---- main loop -------------------------------------------------------

    def execute(self) -> None:
        deadline = time.time() + self.args.duration
        self.provision()
        threading.Thread(target=self.sampler, daemon=True).start()
        self.verify_materializer("initial load")
        while time.time() < deadline:
            self.round += 1
            self.burst()
            if self.rng.random() < 0.3:
                # empty the materializer's buffer cache so the comparison is store-backed
                self.mat.stop("fast"); self.mat.start()
            state = self.verify_materializer("after burst")
            roll = self.rng.random()
            if roll < self.args.branch_probability:
                self.create_branch(state)
            elif roll < self.args.branch_probability + 0.15:
                self.restart_store(crash=self.rng.random() < 0.5)
                self.verify_materializer("after store restart")
            for tl, b, _ in list(self.branches):
                if self.rng.random() < 0.3:
                    self.exercise_branch(tl, b)
            self.check_health()
            self.stats["rounds"] = self.round
        # a final coordinated restart proves the store reopens after the whole history
        self.restart_store(crash=False)
        self.verify_materializer("final, after clean store restart")


def one_run(args: argparse.Namespace, seed: int, root: Path) -> bool:
    root.mkdir(parents=True)
    run = Run(args, seed, root)
    started = time.time()
    failure: Failure | None = None
    try:
        run.execute()
    except Failure as f:
        failure = f
    except KeyboardInterrupt:
        run.teardown(preserve_state=False)
        raise
    except Exception as e:                      # a driver bug is still worth a preserved root
        failure = Failure("driver_exception", repr(e))
        import traceback
        traceback.print_exc()
    if failure:
        try:
            run.dump_diagnostics()
        except Exception:
            pass
    summary = {"seed": seed, "pass": failure is None, "seconds": round(time.time() - started),
               "stats": run.stats, "root": str(root),
               "git": subprocess.run(["git", "-C", str(Path(__file__).parent), "rev-parse", "HEAD"],
                                     capture_output=True, text=True).stdout.strip()}
    if failure:
        summary.update(kind=failure.kind, detail=failure.detail, round=run.round)
        run.event("FAILURE", failure=failure.kind, detail=failure.detail)
    run.teardown(preserve_state=failure is not None)
    (root / ("FAILURE.json" if failure else "PASS.json")).write_text(json.dumps(summary, indent=1) + "\n")
    with open(args.root / "history.jsonl", "a") as h:
        h.write(json.dumps(summary) + "\n")
    print(json.dumps(summary), flush=True)
    return failure is None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--build", required=True, help="meson build directory with a tmp_install")
    p.add_argument("--root", required=True, type=Path,
                   help="working directory; put it on a real filesystem, not tmpfs, so fsync is real")
    p.add_argument("--seed", type=int, default=int(time.time()))
    p.add_argument("--duration", type=int, default=3600, help="seconds per run (one topology lifetime)")
    p.add_argument("--forever", action="store_true", help="run seed, seed+1, ... until --max-failures")
    p.add_argument("--max-failures", type=int, default=5, help="preserved failure roots before stopping")
    p.add_argument("--min-free-gb", type=float, default=50, help="stop --forever below this free space")
    p.add_argument("--scale", type=int, default=5)
    p.add_argument("--clients", type=int, default=4)
    p.add_argument("--burst-min", type=int, default=5)
    p.add_argument("--burst-max", type=int, default=30)
    p.add_argument("--branch-probability", type=float, default=0.25)
    p.add_argument("--max-branches", type=int, default=2)
    p.add_argument("--sync-timeout", type=float, default=1800)
    p.add_argument("--sample-interval", type=float, default=15)
    p.add_argument("--max-store-gb", type=float, default=100)
    p.add_argument("--daemon-arg", action="append", default=[],
                   help="extra pagestore_daemon argument; repeat for each word (experiments)")
    args = p.parse_args()
    args.root = args.root.resolve()
    args.root.mkdir(parents=True, exist_ok=True)

    seed, failures = args.seed, 0
    while True:
        root = args.root / f"run-{seed}"
        ok = one_run(args, seed, root)
        if ok:
            # keep the evidence, drop the bulk
            keep = args.root / "passed" / str(seed)
            keep.mkdir(parents=True, exist_ok=True)
            for name in ("PASS.json", "events.jsonl", "metrics.jsonl"):
                if (root / name).exists():
                    shutil.copy(root / name, keep / name)
            shutil.rmtree(root, ignore_errors=True)
        else:
            failures += 1
        if not args.forever:
            return 0 if ok else 1
        if failures >= args.max_failures:
            print(f"stopping: {failures} preserved failure roots under {args.root}", flush=True)
            return 1
        if shutil.disk_usage(args.root).free / 2**30 < args.min_free_gb:
            print("stopping: free space below --min-free-gb", flush=True)
            return 1
        seed += 1


if __name__ == "__main__":
    sys.exit(main())

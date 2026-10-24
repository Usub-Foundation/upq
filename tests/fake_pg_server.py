#!/usr/bin/env python3
"""Minimal PostgreSQL wire-protocol server for upq's failure-path tests.

Every connection authenticates without a password. The database name picks
the scenario, so one server serves every test:

    <mode>_<n>_<anything>

For the first <n> statements sent to that database name (counted across
connections) the server misbehaves according to <mode>; afterwards every
statement succeeds and returns one row, one int4 column "v" = 1.

    notstarted   ErrorResponse 08P01 "server login has been failing, try
                 again later (server_login_retry)" (pgbouncer's wording)
    drop         close the socket without answering (connection lost)
    hang         never answer (the client's IO deadline has to fire)
    mute         accept the TCP connection, never answer the startup
                 message (the client's connect deadline has to fire)
    deadlock     ErrorResponse 40P01
    ok           behave from the start (n is ignored)

    usage: fake_pg_server.py <port>
"""

import socket
import struct
import sys
import threading

COUNTERS = {}
LOCK = threading.Lock()


def msg(kind: bytes, payload: bytes = b"") -> bytes:
    return kind + struct.pack("!I", len(payload) + 4) + payload


def cstr(s: str) -> bytes:
    return s.encode() + b"\0"


def error(sqlstate: str, text: str) -> bytes:
    return msg(b"E", b"S" + cstr("ERROR") + b"V" + cstr("ERROR") + b"C" + cstr(sqlstate) + b"M" + cstr(text) + b"\0")


def one_row() -> bytes:
    rd = struct.pack("!H", 1) + cstr("v") + struct.pack("!IHIhih", 0, 0, 23, 4, -1, 0)
    return (msg(b"T", rd) + msg(b"D", struct.pack("!H", 1) + struct.pack("!I", 1) + b"1")
            + msg(b"C", cstr("SELECT 1")))


def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError
        buf += chunk
    return buf


def statement_outcome(db: str) -> str:
    parts = db.split("_")
    mode = parts[0]
    n = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    with LOCK:
        seen = COUNTERS.get(db, 0)
        COUNTERS[db] = seen + 1
    if mode == "ok" or seen >= n:
        return "ok"
    return mode


def serve(conn):
    try:
        # Startup: answer SSL / GSS encryption requests with 'N'.
        while True:
            length = struct.unpack("!I", recv_exact(conn, 4))[0]
            body = recv_exact(conn, length - 4)
            code = struct.unpack("!I", body[:4])[0]
            if code in (80877103, 80877104):
                conn.sendall(b"N")
                continue
            break
        params = body[4:].split(b"\0")
        kv = dict(zip(params[0::2], params[1::2]))
        db = kv.get(b"database", b"").decode()
        if db.startswith("mute"):
            while conn.recv(65536):
                pass
            return

        out = msg(b"R", struct.pack("!I", 0))
        for k, v in (("server_version", "17.0"), ("client_encoding", "UTF8"),
                     ("standard_conforming_strings", "on"), ("integer_datetimes", "on"),
                     ("DateStyle", "ISO, MDY")):
            out += msg(b"S", cstr(k) + cstr(v))
        out += msg(b"K", struct.pack("!II", 4242, 1)) + msg(b"Z", b"I")
        conn.sendall(out)

        pending = b""      # extended-protocol reply accumulated until Sync
        outcome = None
        while True:
            kind = recv_exact(conn, 1)
            length = struct.unpack("!I", recv_exact(conn, 4))[0]
            recv_exact(conn, length - 4)
            if kind == b"X":
                return
            if kind in (b"Q", b"P"):  # a statement starts here
                outcome = statement_outcome(db)
                if outcome == "drop":
                    return
                if outcome == "hang":
                    while conn.recv(65536):
                        pass
                    return
            if kind == b"Q":
                if outcome == "ok":
                    conn.sendall(one_row() + msg(b"Z", b"I"))
                elif outcome == "notstarted":
                    conn.sendall(error("08P01", "server login has been failing, try again later "
                                                "(server_login_retry)") + msg(b"Z", b"I"))
                elif outcome == "deadlock":
                    conn.sendall(error("40P01", "deadlock detected") + msg(b"Z", b"I"))
            elif kind == b"P":
                pending = msg(b"1") if outcome == "ok" else b""
            elif kind == b"B":
                if outcome == "ok":
                    pending += msg(b"2")
            elif kind == b"D":
                pass  # RowDescription goes out with the Execute reply
            elif kind == b"E":
                if outcome == "ok":
                    pending += one_row()
                elif outcome == "notstarted":
                    pending += error("08P01", "server login has been failing, try again later "
                                              "(server_login_retry)")
                elif outcome == "deadlock":
                    pending += error("40P01", "deadlock detected")
            elif kind == b"S":
                conn.sendall(pending + msg(b"Z", b"I"))
                pending = b""
    except (ConnectionError, OSError):
        pass
    finally:
        conn.close()


def main():
    port = int(sys.argv[1])
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(128)
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=serve, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()

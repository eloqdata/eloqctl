#!/usr/bin/env python3
"""Stress test: open many concurrent connections to EloqKV and verify cluster health."""
import socket, sys, time, os, threading, argparse

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", type=int, default=6379)
parser.add_argument("--password", default="testpass")
parser.add_argument("--connections", type=int, default=30000)
parser.add_argument("--batch", type=int, default=200, help="connections per thread batch")
args = parser.parse_args()

AUTH_CMD = f"*2\r\n$4\r\nAUTH\r\n${len(args.password)}\r\n{args.password}\r\n"
PING_CMD = "*1\r\n$4\r\nPING\r\n"
results = {"ok": 0, "fail": 0, "errors": []}
lock = threading.Lock()

def worker(thread_id, count):
    """Open count connections, authenticate, ping, disconnect."""
    conns = []
    for i in range(count):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
            s.connect((args.host, args.port))
            s.sendall(AUTH_CMD.encode())
            s.recv(256)  # read AUTH response
            s.sendall(PING_CMD.encode())
            resp = s.recv(256)
            s.close()
            if b"PONG" in resp:
                with lock:
                    results["ok"] += 1
            else:
                with lock:
                    results["fail"] += 1
        except Exception as e:
            with lock:
                results["fail"] += 1
                if len(results["errors"]) < 10:
                    results["errors"].append(f"conn {i}: {e}")

total = args.connections
thread_count = max(1, total // args.batch)
conns_per_thread = args.batch
threads = []
print(f"Opening {total} connections ({thread_count} threads x {conns_per_thread} each)...")
start = time.time()

for tid in range(thread_count):
    t = threading.Thread(target=worker, args=(tid, conns_per_thread))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

elapsed = time.time() - start
rate = total / elapsed if elapsed > 0 else 0
print(f"Done in {elapsed:.1f}s ({rate:.0f} conns/s): {results['ok']} OK, {results['fail']} FAIL")
if results["errors"]:
    for e in results["errors"][:5]:
        print(f"  {e}")
sys.exit(0 if results["fail"] == 0 else 1)

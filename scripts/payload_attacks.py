#!/usr/bin/env python3
"""Brutal adversarial payload suite for the Jobflow API.

Asserts for every attack: (1) status lands in the expected band — no 500s,
no data leaks; (2) error bodies match the standard envelope and leak nothing.
Final liveness probe proves normal flows survive the barrage.
Run: python3 scripts/payload_attacks.py [base_url]
"""
import json, sys, time
import urllib.request as u
import urllib.error as ue

B = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080") + "/api/v1"
PASS, FAIL = [], []
LEAK_MARKERS = ["syntax error", "pg_", "select ", "password", "/users/", "/home/",
                "panicked at", "rust_backtrace", "argon2", "secret"]

def call(method, path, body=None, token=None, headers=None, raw=None):
    req = u.Request(B + path, method=method)
    req.add_header("content-type", "application/json")
    if token: req.add_header("authorization", "Bearer " + token)
    for k, v in (headers or {}).items(): req.add_header(k, v)
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    try:
        r = u.urlopen(req, data)
        return r.status, json.loads(r.read() or b"{}")
    except ue.HTTPError as e:
        raw_body = e.read()
        try: return e.code, json.loads(raw_body)
        except Exception: return e.code, {"raw": raw_body.decode(errors="replace")[:200]}

def check(name, cond, extra=""):
    (PASS if cond else FAIL).append(name)
    print(("PASS" if cond else "FAIL"), name, ("" if cond else extra[:240]))

def envelope_ok(body):
    ok_shape = isinstance(body, dict) and isinstance(body.get("error"), dict) \
        and "code" in body["error"] and "message" in body["error"]
    blob = json.dumps(body).lower() if isinstance(body, dict) else str(body).lower()
    return ok_shape and not any(m in blob for m in LEAK_MARKERS)

def expect(name, statuses, fn):
    try:
        st, body = fn()
    except Exception as ex:
        check(name, False, f"EXCEPTION {type(ex).__name__} {ex}")
        return None
    env_needed = st >= 400
    ok = st in statuses and (envelope_ok(body) or not env_needed)
    check(name, ok, f"got {st}: {json.dumps(body)[:180]}")
    return st, body

ns = str(time.time_ns())
s, a = call("POST", "/auth/register", {"email": f"brutal{ns}@t.io", "password": "password123", "display_name": "Brutal"})
assert s == 201, a
TOK = a["access_token"]
_, org = call("POST", "/organizations", {"name": "Brutal Org", "slug": f"brutal-{ns}"}, TOK)
_, proj = call("POST", "/projects", {"org_id": org["id"], "name": "BP", "slug": "bp"}, TOK)
_, q = call("POST", "/queues", {"project_id": proj["id"], "name": "bq"}, TOK)
QID, PROJ = q["id"], proj["id"]

s, b = call("POST", "/auth/register", {"email": f"victim{ns}@t.io", "password": "password123", "display_name": "V"})
VTOK = b["access_token"]
_, vorg = call("POST", "/organizations", {"name": "Victim", "slug": f"vict-{ns}"}, VTOK)
_, vproj = call("POST", "/projects", {"org_id": vorg["id"], "name": "VP", "slug": "vp"}, VTOK)
_, vq = call("POST", "/queues", {"project_id": vproj["id"], "name": "v-q"}, VTOK)

print("\n== A. PAYLOAD STRUCTURE ==")
deep_raw = ('{"queue_id":"%s","payload":{"a":' % QID) + '{"a":' * 1500 + "1" + "}" * 1500 + "}}"
expect("A1 deep-nesting 1500 levels -> 400", {400}, lambda: call("POST", "/jobs", None, TOK, raw=deep_raw.encode()))
expect("A2 array payload -> 400", {400}, lambda: call("POST", "/jobs", [1,2,3], TOK))
expect("A3 string payload -> 400", {400}, lambda: call("POST", "/jobs", "hello", TOK))
expect("A4 number payload -> 400", {400}, lambda: call("POST", "/jobs", 42, TOK))
big = {"queue_id": QID, "payload": {"blob": "x" * (256*1024)}}
expect("A5 payload >256KiB -> 400/413", {400, 413}, lambda: call("POST", "/jobs", big, TOK))
huge_raw = ('{"queue_id":"%s","payload":{"x":"' % QID) + "y"*(1024*1024) + '"}}'
expect("A6 1MB raw body -> envelope", {400, 413}, lambda: call("POST", "/jobs", None, TOK, raw=huge_raw.encode()))
proto = {"queue_id": QID, "payload": {"type":"echo","__proto__":{"admin":True},"constructor":{"prototype":{"x":1}}}}
st, jb = call("POST", "/jobs", proto, TOK)
check(f"A7 __proto__ keys inert ({st})", st == 202)
nul_key = {"queue_id": QID, "payload": {"type":"echo"}, "idempotency_key": "k\u0000evil"}
expect("A8 NUL byte idempotency key -> 400", {400}, lambda: call("POST", "/jobs", nul_key, TOK))
nul_name = {"project_id": PROJ, "name": "q\u0000evil"}
expect("A9 NUL byte queue name -> 400 not-500", {400}, lambda: call("POST", "/queues", nul_name, TOK))
ctrl_desc = {"project_id": PROJ, "name": "cd-ok", "description": "bell\u0007esc\u001b"}
expect("A10 control chars description -> 400", {400}, lambda: call("POST", "/queues", ctrl_desc, TOK))

print("\n== B. NUMERIC BOUNDS ==")
bad_nums = [
    ("priority", -2147483648), ("priority", 2147483647), ("priority", 5.5),
    ("priority", "5"), ("priority", True), ("max_attempts", 0),
    ("max_attempts", -3), ("max_attempts", 101), ("max_attempts", 2.5),
    ("base_delay_secs", -10), ("max_delay_secs", 9223372036854775807),
]
for i, (field, val) in enumerate(bad_nums):
    b = {"queue_id": QID, "payload": {"type": "echo"}}; b[field] = val
    expect(f"B{i+1} {field}={val!r} -> 400", {400}, lambda bb=b: call("POST", "/jobs", bb, TOK))

print("\n== C. ENUM / INJECTION ==")
for i, bad in enumerate(["FIXED", "Exponential", "fixed'; DROP TABLE jobs;--", "fixed OR 1=1"]):
    expect(f"C{i+1} retry_strategy={bad!r} -> 400", {400},
           lambda bb=bad: call("POST", "/jobs", {"queue_id": QID, "payload": {"type":"echo"}, "retry_strategy": bb}, TOK))
for i, bad in enumerate(["<script>alert(1)</script>", "x'; INSERT INTO users--", "../../etc/passwd"]):
    st, jj = call("POST", "/jobs", {"queue_id": QID, "payload": {"type":"echo","evil":bad}}, TOK)
    check(f"C5.{i+1} hostile string inert ({st})", st == 202)

print("\n== D. STRING FIELDS ==")
expect("D1 queue name 501 chars -> 400", {400},
       lambda: call("POST", "/queues", {"project_id": PROJ, "name": "q"*501}, TOK))
st, qb = call("POST", "/queues", {"project_id": PROJ, "name": "y'; DROP TABLE queues;--"}, TOK)
check(f"D2 SQLi name rejected-or-inert ({st})", st in (201, 400))
if st == 201:
    s2, _ = call("GET", f"/queues/{qb['id']}", token=TOK)
    check("D2b alive after SQLi name", s2 == 200)
st, jj = call("POST", "/jobs", {"queue_id": QID, "payload": {"type":"echo","xss":"<img src=x onerror=alert(1)>"}}, TOK)
check(f"D3 XSS-ish payload inert ({st})", st == 202)

print("\n== E. AUTH BYPASS / IDOR ==")
forged = "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiIxIn0."
s1, _b1 = call("GET", "/organizations", token=forged)
check("E1 alg=none forged JWT -> 401", s1 == 401)
s2, _b2 = call("GET", "/workers", token="AAAAAAAAAA" + TOK[10:])
check("E2 corrupted token -> 401", s2 == 401)
s3, _b3 = call("GET", f"/queues/{vq['id']}", token=TOK)
check(f"E3 cross-tenant read blocked ({s3})", s3 in (403, 404))
s4, _b4 = call("POST", "/jobs", {"queue_id": vq["id"], "payload": {"type":"echo"}}, token=TOK)
check(f"E4 cross-tenant submit blocked ({s4})", s4 in (403, 404))
s5, rr = call("GET", f"/jobs?queue_id={vq['id']}", token=TOK)
rows = rr.get("data", []) if isinstance(rr, dict) else []
check(f"E5 cross-tenant list blocked/empty ({s5})", s5 in (200, 403) and not rows)
s6, ap = call("POST", "/auth/login", {"email": f"brutal{ns}@t.io", "password": "password123"})
s7, _b7 = call("GET", "/workers", token=ap["refresh_token"])
check(f"E7 refresh-as-access -> 401 ({s7})", s7 == 401)

print("\n== F. QUERY PARAMS ==")
expect("F1 page=-5 clamped", {200}, lambda: call("GET", "/jobs?page=-5&page_size=5", token=TOK))
expect("F2 page_size=10000 clamped", {200}, lambda: call("GET", "/jobs?page_size=10000", token=TOK))
expect("F3 status=bogus -> 400", {400}, lambda: call("GET", "/jobs?status=bogus", token=TOK))
expect("F4 worker_id=zzz -> 400", {400}, lambda: call("GET", "/jobs?worker_id=zzz", token=TOK))
many_ids = ",".join(str(i) for i in range(120))
expect("F5 batch-stats 120 ids -> 400", {400}, lambda: call("GET", f"/queues/batch-stats?ids={many_ids}", token=TOK))
dupes = ",".join([QID]*3)
s9, bs = call("GET", f"/queues/batch-stats?ids={dupes}", token=TOK)
check("F6 dup ids deduped (200, <=1 rows)", s9 == 200 and len(bs) <= 1)
garbage = QID + ",zzz"
expect("F7 garbage segment -> 400", {400}, lambda: call("GET", f"/queues/batch-stats?ids={garbage}", token=TOK))
expect("F8 minutes=100000 clamped", {200}, lambda: call("GET", f"/queues/{QID}/throughput?minutes=100000", token=TOK))
expect("F9 minutes=-1 clamped", {200}, lambda: call("GET", f"/queues/{QID}/throughput?minutes=-1", token=TOK))

print("\n== G. HEADERS ==")
sH, _h = call("GET", "/workers", token="Bearer " + TOK)
check(f"G1 'Bearer Bearer' handled ({sH})", sH in (200, 401))
import urllib.error as ue2
try:
    rq = u.Request(B + "/jobs", method="POST")
    rq.add_header("authorization", "Bearer " + TOK)
    rq.add_header("Idempotency-Key", "K" * 3000)
    r = u.urlopen(rq, json.dumps({"queue_id": QID, "payload": {}}).encode())
    check(f"G2 oversized Idem-Key -> 202/400 ({r.status})", r.status in (202, 400))
except ue.HTTPError as e:
    check(f"G2 oversized Idem-Key -> {e.code}", e.code in (400, 413, 431))
except Exception as ex:
    check("G2 oversized Idem-Key", False, str(ex)[:120])

print("\n== H. LIVENESS ==")
sL, h = call("GET", "/health")
check("health ok after barrage", sL == 200 and h.get("status") == "ok")
sN, nj = call("POST", "/jobs", {"queue_id": QID, "payload": {"type":"echo","after":"barrage"}}, token=TOK)
check("normal submission still works", sN == 202)

print("\n" + "=" * 46)
print(f"RESULT: {len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILURES:")
    for f in FAIL: print("  -", f)
sys.exit(1 if FAIL else 0)

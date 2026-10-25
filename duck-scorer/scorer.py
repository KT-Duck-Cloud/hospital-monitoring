#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Duck Cloud 점수 계산기 (duck-scorer)

30초마다 멤버(대기 노드)별 건전도 점수를 계산하고,
운영 중인 VM·앱·DB가 기준 시간(VM·K8s·앱 60초, DB 20초) 넘게 응답이 없으면
점수 1등 후보를 target으로 DR-Control에 보낸다.

- 점수 기준: DR_SCORING_AND_FAILOVER_SPEC 2.1 배점표 (최대 110점)
- 값 출처: Prometheus(node-exporter, mysqld_exporter, blackbox /readyz) + tailscale ping + TCP 6443
- 계산 결과는 :9200/metrics 로 내보내 Grafana에서 볼 수 있다.
- 계산 부분은 Python 3.10 표준 라이브러리만 사용한다. (웹 API는 api.py, FastAPI)
"""
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

KST = timezone(timedelta(hours=9))
ROLES = ("vm1", "vm2")  # vm1 = Master(K8s), vm2 = Worker(DB)


def log(msg):
    print(f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


# ------------------------------------------------------------------ 바깥 세계 (시험 때 가짜로 바꿔 끼움)
class Probe:
    """Prometheus 조회, tailscale ping, TCP 연결 확인"""

    def __init__(self, prom_url, ping_timeout=3, tcp_timeout=3):
        self.prom_url = prom_url.rstrip("/")
        self.ping_timeout = ping_timeout
        self.tcp_timeout = tcp_timeout

    def query(self, promql):
        """결과를 {host: 값} 으로 돌려준다. 실패하면 None."""
        url = f"{self.prom_url}/api/v1/query?" + urllib.parse.urlencode({"query": promql})
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                body = json.load(r)
        except Exception as e:
            log(f"[경고] Prometheus 조회 실패: {e}")
            return None
        out = {}
        for item in body.get("data", {}).get("result", []):
            host = item["metric"].get("host")
            if host:
                out[host] = float(item["value"][1])
        return out

    def query_series(self, promql):
        """라벨을 살린 결과 [(라벨 dict, 값)] / 실패하면 None"""
        url = f"{self.prom_url}/api/v1/query?" + urllib.parse.urlencode({"query": promql})
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                body = json.load(r)
        except Exception as e:
            log(f"[경고] Prometheus 조회 실패: {e}")
            return None
        return [(item["metric"], float(item["value"][1])) for item in body.get("data", {}).get("result", [])]

    PONG = re.compile(r"pong from .* via (?P<via>\S+) in (?P<ms>[\d.]+)ms")

    def ping(self, ip):
        """(직결 여부, 왕복 ms) / 응답 없으면 (None, None)"""
        try:
            p = subprocess.run(["tailscale", "ping", "-c", "1", "--timeout", f"{self.ping_timeout}s", ip],
                               capture_output=True, text=True, timeout=self.ping_timeout + 5)
        except Exception:
            return None, None
        m = self.PONG.search(p.stdout)
        if not m:
            return None, None
        return (not m.group("via").startswith("DERP")), float(m.group("ms"))

    def tcp(self, ip, port):
        try:
            with socket.create_connection((ip, port), timeout=self.tcp_timeout):
                return True
        except OSError:
            return False


# ------------------------------------------------------------------ 점수 계산기
class Scorer:
    def __init__(self, cfg, probe, state_path=None, sender=None, clock=time.time):
        self.cfg = cfg
        self.probe = probe
        self.state_path = state_path
        self.sender = sender or self._http_send
        self.clock = clock
        self.members = {m["name"]: m for m in cfg["members"]}
        self.state = self._load_state()
        self.last = {}          # 마지막 계산 결과 (지표 출력용)
        self.lock = threading.Lock()
        self.sent_total = {}    # (failed_host, target_host, mode) -> 횟수
        self.no_candidate = 0
        self.last_run = 0
        self.db_hold = set()    # exporter 응답이 없어 DB 판정을 보류한 host

    # ---------- 상태 저장 (재시작해도 연속 안정 시간 유지)
    def _load_state(self):
        st = {"members": {}, "active": dict(self.cfg["active"]), "down_since": {}, "sent": {}}
        if self.state_path and os.path.exists(self.state_path):
            try:
                with open(self.state_path) as f:
                    st.update(json.load(f))
            except Exception as e:
                log(f"[경고] 상태 파일을 못 읽어 새로 시작: {e}")
        return st

    def _save_state(self):
        if not self.state_path:
            return
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=1)
        os.replace(tmp, self.state_path)

    @staticmethod
    def _app_map(series):
        """{멤버(cluster): {앱: 값}} / 조회 실패면 None"""
        if series is None:
            return None
        out = {}
        for m, v in series:
            if m.get("cluster") and m.get("app"):
                out.setdefault(m["cluster"], {})[m["app"]] = v
        return out

    # ---------- 한 바퀴
    def run_once(self):
        now = self.clock()
        win = int(self.cfg["failover_sec"])
        dbwin = int(self.cfg.get("db_failover_sec", win))
        q = {
            "up": self.probe.query('up{job="node-team"}'),
            "up_win": self.probe.query(f'max_over_time(up{{job="node-team"}}[{win}s])'),
            "db": self.probe.query('mysql_up'),
            "db_win": self.probe.query(f'max_over_time(mysql_up[{dbwin}s])'),
            # exporter가 판정 구간 내내 응답했는지 (한 번이라도 놓치면 DB 판정 보류)
            "exp_win": self.probe.query(f'min_over_time(up{{job="mysql"}}[{dbwin}s])'),
        }
        appwin = int(self.cfg.get("app_failover_sec", win))
        q["app_now"] = self._app_map(self.probe.query_series('probe_success{job="app-readyz"}'))
        q["app_win"] = self._app_map(self.probe.query_series(f'max_over_time(probe_success{{job="app-readyz"}}[{appwin}s])'))
        q["app_code"] = self._app_map(self.probe.query_series('probe_http_status_code{job="app-readyz"}'))
        q.update({
            "mem": self.probe.query('node_memory_MemAvailable_bytes{job="node-team"}'),
        })
        if q["up"] is None:
            log("[보류] Prometheus에 연결할 수 없어 이번 바퀴는 판단하지 않음")
            return

        # 온라인 VM만 ping · 6443 확인 (동시에)
        online = {}
        for name, m in self.members.items():
            for r in ROLES:
                vm = m.get(r)
                if vm and q["up"].get(vm["host"]) == 1:
                    online[vm["host"]] = vm["ip"]
        with ThreadPoolExecutor(max_workers=8) as ex:
            pings = dict(zip(online, ex.map(self.probe.ping, online.values())))
            vm1s = {m["vm1"]["host"]: m["vm1"]["ip"] for m in self.members.values()
                    if m.get("vm1") and m["vm1"]["host"] in online}
            k8s = dict(zip(vm1s, ex.map(lambda ip: self.probe.tcp(ip, 6443), vm1s.values())))

        results = {name: self._score(name, m, q, pings, k8s, now) for name, m in self.members.items()}
        failed = self._detect_failed(q, k8s, now)
        self._failover(failed, results, now)

        with self.lock:
            self.last = {"results": results, "failed": failed, "pings": pings, "k8s": k8s}
            self.last_run = now
        self._save_state()

    # ---------- 멤버 점수 (스펙 2.1 / 2.2)
    def _score(self, name, m, q, pings, k8s, now):
        c = self.cfg
        vm1, vm2 = m.get("vm1"), m.get("vm2")
        on = lambda vm: bool(vm) and q["up"].get(vm["host"]) == 1
        healthy = on(vm1) and on(vm2)
        st = self.state["members"].setdefault(name, {"healthy_since": None, "unhealthy_since": None})
        if healthy:
            st["healthy_since"] = st["healthy_since"] or now
            st["unhealthy_since"] = None
        else:
            st["unhealthy_since"] = st["unhealthy_since"] or now
            st["healthy_since"] = None
        stable = now - st["healthy_since"] if healthy else 0

        parts = {k: 0.0 for k in ("base", "stable", "direct", "k8s", "db", "mem", "latency")}
        why = []
        app_down = []
        if not healthy:
            why.append("VM-1 오프라인" if not on(vm1) else "VM-2 오프라인")
        else:
            parts["base"] = 40.0
            parts["stable"] = min(stable / 60.0, 60.0) / 2.0
            vms = [vm1, vm2]
            direct = [pings.get(v["host"], (None, None))[0] for v in vms]
            if all(d is True for d in direct):
                parts["direct"] = 10.0
            else:
                why.append("중계 경로" if any(d is False for d in direct) else "ping 응답 없음")
            if k8s.get(vm1["host"]):
                parts["k8s"] = 10.0
            else:
                why.append("K8s API(6443) 응답 없음")
            if q["db"] and q["db"].get(vm2["host"]) == 1:
                parts["db"] = 10.0
            else:
                why.append("MySQL 미준비")
            mem = [q["mem"].get(v["host"]) for v in vms] if q["mem"] else [None, None]
            if all(x is not None for x in mem):
                parts["mem"] = round(min(sum(mem) / 1024 ** 3, 8.0) * 1.25, 2)
            else:
                why.append("메모리 정보 없음")
            rtts = [pings.get(v["host"], (None, None))[1] for v in vms]
            rtts = [r for r in rtts if r is not None]
            if rtts and max(rtts) > c["rtt_limit_ms"]:
                parts["latency"] = -10.0
                why.append(f"지연 {max(rtts):.0f}ms")
            # 앱 준비 상태 (감시 대상인 멤버만). 하나라도 준비 안 되면 VM-1 역할 후보에서 제외
            apps = (q.get("app_now") or {}).get(name)
            app_down = sorted(a for a, v in (apps or {}).items() if v != 1)
            if app_down:
                why.append("앱 미준비 (" + ", ".join(app_down) + ")")
            if stable < c["stable_sec"]:
                why.append(f"안정 확인 중 {int(stable // 60)}/{c['stable_sec'] // 60}분")
        score = round(sum(parts.values()), 1)
        if not healthy:
            app_down = []
        unhealthy_for = now - st["unhealthy_since"] if st["unhealthy_since"] else 0
        return {
            "site": m["site"], "healthy": healthy, "stable_min": int(stable // 60),
            "eligible": healthy and stable >= c["stable_sec"],
            "demoted": (not healthy) and unhealthy_for >= c["demote_sec"],
            "score": score, "parts": parts, "why": why,
            "app_down": app_down if healthy else [],
        }

    # ---------- 운영 중인 VM 장애 판정 (기준 시간 동안 계속 실패)
    def _detect_failed(self, q, k8s, now):
        win = self.cfg["failover_sec"]
        failed = []
        for role, mname in self.state["active"].items():
            vm = self.members[mname][role]
            host = vm["host"]
            node_dead = q["up_win"] is not None and q["up_win"].get(host, 0) == 0
            if role == "vm1":
                # 6443은 Prometheus에 없으니 계산기가 직접 시간을 잰다
                key = f"k8s:{host}"
                if q["up"].get(host) == 1 and not k8s.get(host):
                    self.state["down_since"].setdefault(key, now)
                else:
                    self.state["down_since"].pop(key, None)
                svc_dead = key in self.state["down_since"] and now - self.state["down_since"][key] >= win
                reason = "노드 응답 없음" if node_dead else "K8s API 응답 없음"
                # 앱: 하나라도 기준 시간 내내 준비 실패. 단 503(앱은 떴는데 DB 접속 실패)은 DB 규칙이 맡는다
                if not node_dead and not svc_dead and q.get("app_win") is not None:
                    win_apps = q["app_win"].get(mname, {})
                    codes = (q.get("app_code") or {}).get(mname, {})
                    dead_apps = sorted(a for a, v in win_apps.items() if v == 0 and codes.get(a) != 503)
                    if dead_apps:
                        svc_dead = True
                        reason = "앱 응답 없음 (" + ", ".join(dead_apps) + ")"
            else:
                # exporter가 구간 내내 살아 있으면서 MySQL 접속이 계속 실패해야 DB 장애
                exp_ok = q["exp_win"] is not None and q["exp_win"].get(host) == 1
                db_zero = q["db_win"] is not None and q["db_win"].get(host, 0) == 0
                svc_dead = exp_ok and db_zero
                # VM은 켜져 있는데 exporter만 응답이 없을 때만 보류 (VM이 꺼졌으면 60초 VM 기준으로 판정)
                if not node_dead and not exp_ok and q["up"].get(host) == 1:
                    if host not in self.db_hold:
                        log(f"[보류] {host} mysqld_exporter 응답 없음 — DB 장애 판정을 보류함 (전환하지 않음)")
                    self.db_hold.add(host)
                else:
                    self.db_hold.discard(host)
                reason = "노드 응답 없음" if node_dead else "DB 응답 없음"
            if node_dead or svc_dead:
                failed.append({"role": role, "member": mname, "host": host, "ip": vm["ip"], "reason": reason})
            else:
                self.state["sent"].pop(host, None)  # 회복하면 다시 보낼 수 있게
                self.state.setdefault("retry_at", {}).pop(host, None)
        return failed

    # ---------- 후보 선택 (전송과 API 미리보기가 같은 규칙을 씀)
    def best_candidate(self, role, exclude_member, results=None):
        """그 역할을 넘겨받을 1등 후보 (점수, 멤버) / 없으면 None
        장애 난 멤버는 빼고, VM-1은 K8s API, VM-2는 DB가 살아 있는 후보만.
        prefer_onprem(기본 켜짐)이면 온프레미스 후보가 있을 때 AWS는 제외한다."""
        if results is None:
            with self.lock:
                results = self.last.get("results", {})
        need = "k8s" if role == "vm1" else "db"
        cands = [(r["score"], n) for n, r in results.items()
                 if r["eligible"] and n != exclude_member and self.members[n].get(role)
                 and r["parts"][need] > 0
                 and not (role == "vm1" and r.get("app_down"))]
        # 온프레미스 우선: 자격 있는 온프레미스 후보가 하나라도 있으면 AWS는 고르지 않는다
        if self.cfg.get("prefer_onprem", True):
            onprem = [c for c in cands if self.members[c[1]]["site"] == "on-prem"]
            cands = onprem or cands
        return max(cands) if cands else None

    def make_message(self, role, best, failed_ip, failed_host, now):
        """DR-Control에 보내는 target 메시지 (형식 고정)"""
        tvm = self.members[best][role]
        return {
            "target": {"site": self.members[best]["site"], "ip": tvm["ip"], "host": tvm["host"]},
            "failed": {"ip": failed_ip, "host": failed_host},
            "timestamp": datetime.fromtimestamp(now, KST).isoformat(timespec="seconds"),
        }

    # ---------- target 전송
    def _failover(self, failed, results, now):
        self.no_candidate = 0
        for f in failed:
            if f["host"] in self.state["sent"]:
                continue  # 같은 장애로 두 번 보내지 않음
            retry_at = self.state.setdefault("retry_at", {}).get(f["host"], 0)
            if now < retry_at:
                continue  # 실패한 전송은 retry_sec 뒤에 다시
            pick = self.best_candidate(f["role"], f["member"], results)
            if not pick:
                self.no_candidate = 1
                log(f"[경고] {f['host']} 장애({f['reason']}) — 자격 있는 후보 없음, 전송하지 않음")
                continue
            score, best = pick
            msg = self.make_message(f["role"], best, f["ip"], f["host"], now)
            tvm = self.members[best][f["role"]]
            ok, mode = self.sender(msg)
            key = (f["host"], tvm["host"], mode)
            self.sent_total[key] = self.sent_total.get(key, 0) + 1
            log(f"[전환] {f['host']}({f['reason']}) → {tvm['host']} ({best}, {score}점) [{mode}] {'성공' if ok else '실패'}"
                f" {json.dumps(msg, ensure_ascii=False)}")
            if ok:
                self.state["sent"][f["host"]] = now
                self.state["active"][f["role"]] = best
                self.state["retry_at"].pop(f["host"], None)
            else:
                self.state["retry_at"][f["host"]] = now + int(self.cfg.get("retry_sec", 60))

    def _http_send(self, msg):
        s = self.cfg["send"]
        if not s.get("enabled"):
            return True, "dry_run"
        headers = {"Content-Type": "application/json", "User-Agent": "duck-scorer/2.1"}
        if s.get("token_file"):
            with open(s["token_file"]) as f:
                headers["Authorization"] = "Bearer " + f.read().strip()
        data = json.dumps(msg, ensure_ascii=False).encode()
        for attempt in range(3):
            try:
                req = urllib.request.Request(s["url"], data=data, headers=headers, method="POST")
                with urllib.request.urlopen(req, timeout=10) as r:
                    body = r.read(65536).decode("utf-8", "replace")
                    if 200 <= r.status < 300:
                        if self._looks_like_guide(body):
                            log("[경고] 응답이 주소 안내문이에요 — DR-Control이 메시지를 처리하지 않은 것으로 봄")
                            return False, "ignored"
                        log(f"[전송] 응답 {r.status}: {body[:200]}")
                        return True, "sent"
            except Exception as e:
                log(f"[경고] 전송 실패 {attempt + 1}/3: {e}")
            time.sleep(2)
        return False, "sent"

    @staticmethod
    def _looks_like_guide(body):
        """/api/gateways의 안내문(게이트웨이 목록)이 그대로 돌아왔는지"""
        try:
            d = json.loads(body)
        except Exception:
            return False
        return isinstance(d, dict) and "gateways" in d and "public_domains" in d

    # ---------- API용 읽기 전용 정보
    def scores(self):
        with self.lock:
            res = self.last.get("results", {})
            return {n: dict(r, parts=dict(r["parts"]), why=list(r["why"])) for n, r in res.items()}

    def active(self):
        with self.lock:
            failed = {f["host"]: f for f in self.last.get("failed", [])}
        out = {}
        for role, mname in dict(self.state["active"]).items():
            vm = self.members[mname][role]
            f = failed.get(vm["host"])
            out[role] = {"member": mname, "host": vm["host"], "ip": vm["ip"],
                         "failed": bool(f), "reason": f["reason"] if f else None,
                         "db_check_hold": vm["host"] in self.db_hold}
        return out

    def preview(self, role):
        """지금 운영 중인 그 역할의 VM이 장애가 나면 보낼 메시지 (보내지 않음)"""
        mname = self.state["active"][role]
        vm = self.members[mname][role]
        pick = self.best_candidate(role, mname)
        if not pick:
            return None
        score, best = pick
        return {"role": role, "candidate": best, "score": score,
                "message": self.make_message(role, best, vm["ip"], vm["host"], self.clock())}

    # ---------- /metrics
    def metrics(self):
        with self.lock:
            last, run = self.last, self.last_run
        L = ["# HELP duck_member_score 멤버 건전도 점수 (최대 110)", "# TYPE duck_member_score gauge"]
        esc = lambda s: str(s).replace("\\", "\\\\").replace('"', '\\"')
        for n, r in last.get("results", {}).items():
            lb = f'member="{n}",site="{r["site"]}"'
            L.append(f"duck_member_score{{{lb}}} {r['score']}")
            L.append(f"duck_member_eligible{{{lb}}} {int(r['eligible'])}")
            L.append(f"duck_member_healthy{{{lb}}} {int(r['healthy'])}")
            L.append(f"duck_member_stable_minutes{{{lb}}} {r['stable_min']}")
            for k, v in r["parts"].items():
                L.append(f'duck_member_score_part{{{lb},item="{k}"}} {v}')
            L.append(f'duck_member_info{{{lb},why="{esc(" · ".join(r["why"]) or "정상")}"}} 1')
            L.append(f"duck_member_app_ready{{{lb}}} {int(r['healthy'] and not r.get('app_down'))}")
        for h, (d, ms) in last.get("pings", {}).items():
            L.append(f'duck_vm_direct{{host="{h}"}} {int(bool(d))}')
            if ms is not None:
                L.append(f'duck_vm_rtt_ms{{host="{h}"}} {ms}')
        for h, ok in last.get("k8s", {}).items():
            L.append(f'duck_vm_k8s_api_up{{host="{h}"}} {int(ok)}')
        for role, mname in self.state["active"].items():
            L.append(f'duck_active{{role="{role}",member="{mname}",host="{self.members[mname][role]["host"]}"}} 1')
        failed_hosts = {f["host"] for f in last.get("failed", [])}
        for role, mname in self.state["active"].items():
            h = self.members[mname][role]["host"]
            L.append(f'duck_active_failed{{role="{role}",host="{h}"}} {int(h in failed_hosts)}')
        L.append(f"duck_failover_no_candidate {self.no_candidate}")
        for role, mname in self.state["active"].items():
            if role == "vm2":
                h = self.members[mname][role]["host"]
                L.append(f'duck_db_check_hold{{host="{h}"}} {int(h in self.db_hold)}')
        for (fh, th, mode), cnt in self.sent_total.items():
            L.append(f'duck_failover_sent_total{{failed="{fh}",target="{th}",mode="{mode}"}} {cnt}')
        L.append(f"duck_scorer_last_run_timestamp_seconds {run}")
        return "\n".join(L) + "\n"


def serve(scorer, addr):
    host, port = addr.rsplit(":", 1)

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_response(404); self.end_headers(); return
            body = scorer.metrics().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = HTTPServer((host, int(port)), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log(f"지표 주소: http://{addr}/metrics")


def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "/etc/duck-scorer/config.json"
    with open(cfg_path) as f:
        cfg = json.load(f)
    state_dir = os.environ.get("STATE_DIRECTORY", "/var/lib/duck-scorer")
    os.makedirs(state_dir, exist_ok=True)
    scorer = Scorer(cfg, Probe(cfg["prometheus"]), os.path.join(state_dir, "state.json"))
    serve(scorer, cfg["listen"])
    log(f"시작 — {cfg['interval_sec']}초마다 계산, 장애 기준 {cfg['failover_sec']}초, "
        f"전송 {'켜짐' if cfg['send'].get('enabled') else '꺼짐(dry-run)'}")
    while True:
        t0 = time.time()
        try:
            scorer.run_once()
            for n, r in scorer.last.get("results", {}).items():
                log(f"  {n:4} {r['score']:6.1f}점  후보={'O' if r['eligible'] else 'X'}  {' · '.join(r['why']) or '정상'}")
        except Exception as e:
            log(f"[오류] {e!r}")
        time.sleep(max(1, cfg["interval_sec"] - (time.time() - t0)))


if __name__ == "__main__":
    main()

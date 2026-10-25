# -*- coding: utf-8 -*-
"""가짜 Prometheus · ping · 6443 으로 점수 계산기를 시험한다."""
import json, os, sys, tempfile, unittest
sys.path.insert(0, os.path.dirname(__file__))
import scorer as S

GB = 1024 ** 3
_d = os.path.dirname(__file__)
CFG = json.load(open(os.path.join(_d, "config.json") if os.path.exists(os.path.join(_d, "config.json")) else os.path.join(_d, "config.example.json")))
CFG["listen"] = "127.0.0.1:0"
HOSTS = ["t3-vm1", "t3-vm2", "t5-vm1", "t5-vm2", "t8-vm1", "t8-vm2"]
IP = {m[r]["host"]: m[r]["ip"] for m in CFG["members"] for r in ("vm1", "vm2")}


class World:
    """테스트가 조절하는 가짜 세상"""
    def __init__(self):
        self.t = 1_000_000.0
        self.up = {h: 1 for h in HOSTS}                 # 지금 node-exporter
        self.down_from = {}                              # host -> 꺼진 시각 (up_win 계산)
        self.db = {"t3-vm2": 1, "t5-vm2": 1, "t8-vm2": 1}
        self.db_down_from = {}
        self.exp = {"t3-vm2": 1, "t5-vm2": 1, "t8-vm2": 1}     # mysqld_exporter 응답
        self.exp_down_from = {}
        # 앱 /readyz (blackbox) — 감시 대상은 t3, t5만
        self.app = {(c, a): 1 for c in ("t3", "t5") for a in ("reservation", "auth", "stats")}
        self.app_down_from = {}
        self.app_code = {}     # (c, a) -> 응답 코드 (실패 시)
        self.mem = {"t3-vm1": 4 * GB, "t3-vm2": 4 * GB, "t5-vm1": 4 * GB, "t5-vm2": 4 * GB,
                    "t8-vm1": 1.0 * GB, "t8-vm2": 1.4 * GB}
        self.ping = {h: (True, 3.0) for h in HOSTS}
        self.ping.update({"t5-vm1": (True, 18.0), "t5-vm2": (True, 18.0),
                          "t8-vm1": (True, 144.0), "t8-vm2": (True, 226.0)})
        self.k8s = {h: True for h in HOSTS}
        self.prom_ok = True

    def set_up(self, host, v):
        self.up[host] = v
        if v == 0: self.down_from.setdefault(host, self.t)
        else: self.down_from.pop(host, None)

    def set_exp(self, host, v):
        self.exp[host] = v
        if v == 0: self.exp_down_from.setdefault(host, self.t)
        else: self.exp_down_from.pop(host, None)

    def set_app(self, member, app, v, code=0):
        k = (member, app)
        self.app[k] = v
        if v == 0:
            self.app_down_from.setdefault(k, self.t); self.app_code[k] = code
        else:
            self.app_down_from.pop(k, None); self.app_code.pop(k, None)

    def set_db(self, host, v):
        self.db[host] = v
        if v == 0: self.db_down_from.setdefault(host, self.t)
        else: self.db_down_from.pop(host, None)


class FakeProbe:
    def __init__(self, w): self.w = w
    def query(self, q):
        w = self.w
        if not w.prom_ok: return None
        win = CFG["failover_sec"]
        dbwin = CFG.get("db_failover_sec", win)
        exp_alive = lambda h: w.up[h] and w.exp[h]
        if q.startswith("min_over_time(up{job=\"mysql\"}"):
            # 구간 안에 한 번이라도 응답이 없었으면 0
            out = {}
            for h in w.exp:
                down_at = [w.down_from.get(h), w.exp_down_from.get(h)]
                out[h] = 0 if (not exp_alive(h) or any(d is not None and w.t - d < dbwin + 1 for d in down_at)) else 1
            return out
        if q.startswith("max_over_time(up"):
            return {h: (0 if h in w.down_from and w.t - w.down_from[h] >= win else 1) for h in HOSTS}
        if q.startswith("up"): return dict(w.up)
        if q.startswith("max_over_time(mysql_up"):
            # exporter가 없으면 시계열 자체가 없음
            return {h: (0 if h in w.db_down_from and w.t - w.db_down_from[h] >= dbwin else 1)
                    for h in w.db if exp_alive(h)}
        if q.startswith("mysql_up"): return {h: v for h, v in w.db.items() if exp_alive(h)}
        if q.startswith("node_memory"): return {h: v for h, v in w.mem.items() if w.up[h]}
        raise AssertionError(q)
    def query_series(self, q):
        w = self.w
        if not w.prom_ok: return None
        appwin = CFG.get("app_failover_sec", CFG["failover_sec"])
        alive = lambda c, a: w.app[(c, a)] and w.up[f"{c}-vm1"]
        lab = lambda c, a: {"cluster": c, "app": a, "host": f"{c}-vm1", "job": "app-readyz"}
        out = []
        for (c, a) in w.app:
            if q.startswith("max_over_time(probe_success"):
                d = w.app_down_from.get((c, a)) if w.up[f"{c}-vm1"] else w.down_from.get(f"{c}-vm1")
                v = 0 if (not alive(c, a) and d is not None and w.t - d >= appwin) else 1
            elif q.startswith("probe_success"):
                v = 1 if alive(c, a) else 0
            elif q.startswith("probe_http_status_code"):
                v = 200 if alive(c, a) else w.app_code.get((c, a), 0)
            else:
                raise AssertionError(q)
            out.append((lab(c, a), v))
        return out

    def ping(self, ip):
        h = [k for k, v in IP.items() if v == ip][0]
        return self.w.ping[h] if self.w.up[h] else (None, None)
    def tcp(self, ip, port):
        h = [k for k, v in IP.items() if v == ip][0]
        return self.w.up[h] == 1 and self.w.k8s[h]


class T(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.sent = []
        self.tmp = tempfile.mkdtemp()
        self.s = S.Scorer(json.loads(json.dumps(CFG)), FakeProbe(self.w), os.path.join(self.tmp, "st.json"),
                          sender=lambda m: (self.sent.append(m) or (True, "dry_run")), clock=lambda: self.w.t)

    def tick(self, sec=30, n=1):
        for _ in range(n):
            self.w.t += sec
            self.s.run_once()

    def warm(self, minutes=61):
        self.s.run_once()
        self.tick(60, minutes)

    def res(self, name): return self.s.last["results"][name]

    # ---- 스펙 2.3 실측 사례 재현
    def test_spec_t5_110(self):
        self.warm()
        self.assertEqual(self.res("t5")["score"], 110.0)
        self.assertTrue(self.res("t5")["eligible"])

    def test_spec_t8_83(self):
        self.w.set_db("t8-vm2", 0)  # 스펙: MySQL 미준비
        self.warm()
        r = self.res("t8")
        self.assertEqual(r["parts"]["mem"], 3.0)
        self.assertEqual(r["parts"]["latency"], -10.0)
        self.assertEqual(r["score"], 83.0)

    def test_relay_loses_direct(self):
        self.w.ping["t5-vm2"] = (False, 40.0)
        self.warm()
        self.assertEqual(self.res("t5")["score"], 100.0)
        self.assertIn("중계 경로", self.res("t5")["why"])

    # ---- 후보 자격 (10분)
    def test_not_eligible_before_10min(self):
        self.s.run_once(); self.tick(60, 9)
        self.assertFalse(self.res("t5")["eligible"])
        self.tick(60, 1)
        self.assertTrue(self.res("t5")["eligible"])

    # ---- 1단계: t3-vm1 다운 → t5-vm1
    def test_vm1_down_sends_t5_vm1_once(self):
        self.warm()
        self.w.set_up("t3-vm1", 0)
        self.tick(30)                       # 30초: 아직 아님
        self.assertEqual(self.sent, [])
        self.tick(30)                       # 60초: 전송
        self.assertEqual(len(self.sent), 1)
        m = self.sent[0]
        self.assertEqual(m["target"], {"site": "on-prem", "ip": "100.64.0.51", "host": "t5-vm1"})
        self.assertEqual(m["failed"], {"ip": "100.64.0.31", "host": "t3-vm1"})
        self.assertTrue(m["timestamp"].endswith("+09:00"))
        self.assertEqual(self.s.state["active"]["vm1"], "t5")
        self.tick(30, 5)                    # 같은 장애로 다시 안 보냄
        self.assertEqual(len(self.sent), 1)

    # ---- VM-2 DB만 다운 → t5-vm2
    def test_db_down_sends_vm2(self):
        self.warm()
        self.w.set_db("t3-vm2", 0)
        self.tick(10, 1)
        self.assertEqual(self.sent, [])     # 10초: 아직 아님
        self.tick(10, 1)                    # 20초: 전송
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0]["target"]["host"], "t5-vm2")
        self.assertEqual(self.sent[0]["failed"]["host"], "t3-vm2")

    # ---- 둘 다 다운 → 메시지 2개 (VM-1 쌍, VM-2 쌍)
    def test_both_down_two_messages(self):
        self.warm()
        self.w.set_up("t3-vm1", 0); self.w.set_up("t3-vm2", 0); self.w.set_db("t3-vm2", 0)
        self.tick(30, 2)
        pairs = sorted((m["failed"]["host"], m["target"]["host"]) for m in self.sent)
        self.assertEqual(pairs, [("t3-vm1", "t5-vm1"), ("t3-vm2", "t5-vm2")])

    # ---- 2단계: 1조 전부 다운 → AWS (C안: 그냥 1등)
    def test_all_onprem_down_goes_aws(self):
        self.warm()
        for h in ["t3-vm1", "t3-vm2", "t5-vm1", "t5-vm2"]: self.w.set_up(h, 0)
        self.w.set_db("t3-vm2", 0)
        self.tick(30, 2)
        hosts = sorted(m["target"]["host"] for m in self.sent)
        self.assertEqual(hosts, ["t8-vm1", "t8-vm2"])
        self.assertTrue(all(m["target"]["site"] == "aws" for m in self.sent))

    def test_no_candidate_no_send(self):
        self.w.set_db("t8-vm2", 0)          # AWS DB 미준비 → VM-2 후보 없음
        self.warm()
        for h in ["t5-vm1", "t5-vm2"]: self.w.set_up(h, 0)
        self.tick(60, 6)
        self.w.set_db("t3-vm2", 0)
        self.tick(30, 2)
        self.assertEqual(self.sent, [])
        self.assertEqual(self.s.no_candidate, 1)

    # ---- mon이 사설망에서 빠짐 (전부 0) → 오판 전송 없음
    def test_mon_isolated_no_send(self):
        self.warm()
        for h in HOSTS: self.w.set_up(h, 0)
        self.tick(30, 4)
        self.assertEqual(self.sent, [])

    # ---- Prometheus 연결 실패 → 판단 보류
    def test_prometheus_down_hold(self):
        self.warm()
        self.w.prom_ok = False
        self.w.set_up("t3-vm1", 0)
        self.tick(30, 4)
        self.assertEqual(self.sent, [])

    # ---- 6443만 죽음 (노드는 살아 있음) → 60초 뒤 전송
    def test_k8s_api_down(self):
        self.warm()
        self.w.k8s["t3-vm1"] = False
        self.tick(30, 1); self.assertEqual(self.sent, [])
        self.tick(30, 2); self.assertEqual(self.sent[0]["failed"]["host"], "t3-vm1")

    # ---- 상태 파일로 재시작 후에도 연속 안정 시간 유지
    def test_state_survives_restart(self):
        self.warm()
        s2 = S.Scorer(json.loads(json.dumps(CFG)), FakeProbe(self.w), os.path.join(self.tmp, "st.json"),
                      sender=lambda m: (True, "dry_run"), clock=lambda: self.w.t)
        self.w.t += 30; s2.run_once()
        self.assertEqual(s2.last["results"]["t5"]["score"], 110.0)


    # ---- 온프레미스 우선: t5가 켜진 지 15분(87.5점) < AWS(92점)여도 t5
    def _t5_restarted_15min(self):
        self.warm()
        for h in ["t5-vm1", "t5-vm2"]: self.w.set_up(h, 0)
        self.tick(60, 2)
        for h in ["t5-vm1", "t5-vm2"]: self.w.set_up(h, 1)
        self.tick(60, 15)
        self.assertLess(self.res("t5")["score"], self.res("t8")["score"])
        self.assertTrue(self.res("t5")["eligible"])
        self.w.set_up("t3-vm1", 0)
        self.tick(30, 2)

    def test_prefer_onprem_on(self):
        self._t5_restarted_15min()
        self.assertEqual(self.sent[0]["target"]["host"], "t5-vm1")

    def test_prefer_onprem_off(self):
        self.s.cfg["prefer_onprem"] = False
        self._t5_restarted_15min()
        self.assertEqual(self.sent[0]["target"]["host"], "t8-vm1")

    def test_prefer_onprem_db_only_aws(self):
        # t5 DB만 죽으면 VM-2 역할을 받을 온프레미스 후보가 없으므로 AWS
        self.warm()
        self.w.set_db("t5-vm2", 0)
        self.tick(30, 1)
        self.w.set_db("t3-vm2", 0)
        self.tick(30, 2)
        self.assertEqual(self.sent[0]["target"]["host"], "t8-vm2")


    # ---- DB 20초 기준: 잠깐(10초) 끊겼다 돌아오면 전송 안 함
    def test_db_blip_10s_no_send(self):
        self.warm()
        self.w.set_db("t3-vm2", 0)
        self.tick(10, 1)
        self.w.set_db("t3-vm2", 1)
        self.tick(10, 6)
        self.assertEqual(self.sent, [])

    # ---- exporter만 꺼짐 (MySQL 정상) → 보류, 전송 안 함
    def test_exporter_down_holds(self):
        self.warm()
        self.w.set_exp("t3-vm2", 0)
        self.tick(10, 12)                   # 2분
        self.assertEqual(self.sent, [])
        self.assertIn("t3-vm2", self.s.db_hold)
        self.assertIn('duck_db_check_hold{host="t3-vm2"} 1', self.s.metrics())
        self.w.set_exp("t3-vm2", 1)
        self.tick(10, 3)
        self.assertNotIn("t3-vm2", self.s.db_hold)

    # ---- exporter와 VM이 같이 꺼짐 → VM 기준(60초)으로 전송
    def test_vm2_node_down_still_sends(self):
        self.warm()
        self.w.set_up("t3-vm2", 0)
        self.tick(10, 5)                    # 50초: 아직 아님 (VM은 60초 기준)
        self.assertEqual(self.sent, [])
        self.tick(10, 1)
        self.assertEqual(self.sent[0]["failed"]["host"], "t3-vm2")


    # ---- 4번: 운영 멤버 앱 하나가 60초 죽으면 VM-1 역할 전환
    def test_app_down_sends_vm1(self):
        self.warm()
        self.w.set_app("t3", "reservation", 0)
        self.tick(10, 5)
        self.assertEqual(self.sent, [])         # 50초: 아직
        self.tick(10, 2)
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0]["failed"]["host"], "t3-vm1")
        self.assertEqual(self.sent[0]["target"]["host"], "t5-vm1")

    def test_app_503_left_to_db_rule(self):
        # 앱이 503(DB 접속 실패)이면 앱 규칙으로는 보내지 않음
        self.warm()
        for a in ("reservation", "auth", "stats"): self.w.set_app("t3", a, 0, code=503)
        self.tick(10, 9)
        self.assertEqual(self.sent, [])

    def test_app_blip_no_send(self):
        self.warm()
        self.w.set_app("t3", "auth", 0)
        self.tick(10, 4)
        self.w.set_app("t3", "auth", 1)
        self.tick(10, 6)
        self.assertEqual(self.sent, [])

    def test_standby_app_down_excluded(self):
        # 대기 멤버(t5) 앱이 미준비면 VM-1 후보에서 제외 → AWS
        self.warm()
        self.w.set_app("t5", "stats", 0)
        self.tick(10, 1)
        self.assertIn("앱 미준비 (stats)", self.res("t5")["why"])
        self.w.set_up("t3-vm1", 0)
        self.tick(10, 7)
        self.assertEqual(self.sent[0]["target"]["host"], "t8-vm1")

    # ---- 5번: 안내문 응답은 실패로 보고 60초 뒤 다시
    def test_guide_response_is_failure_and_retry(self):
        calls = []
        self.s.sender = lambda m: (calls.append(m) or (False, "ignored"))
        self.warm()
        self.w.set_up("t3-vm1", 0)
        self.tick(10, 6)                        # 60초: 첫 전송 (안내문 → 실패)
        self.assertEqual(len(calls), 1)
        self.tick(10, 5)                        # 그 뒤 50초: 아직 다시 안 보냄
        self.assertEqual(len(calls), 1)
        self.tick(10, 1)                        # 60초 뒤: 다시
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.s.state["active"]["vm1"], "t3")   # 성공 전까지 운영 노드 유지

    def test_looks_like_guide(self):
        guide = json.dumps({"title": "x", "gateways": [], "public_domains": {}})
        self.assertTrue(S.Scorer._looks_like_guide(guide))
        self.assertFalse(S.Scorer._looks_like_guide('{"accepted": true}'))
        self.assertFalse(S.Scorer._looks_like_guide("ok"))

    # ---- /metrics 형식
    def test_metrics_text(self):
        self.warm()
        txt = self.s.metrics()
        self.assertIn('duck_member_score{member="t5",site="on-prem"} 110.0', txt)
        self.assertIn('duck_vm_rtt_ms{host="t8-vm1"} 144.0', txt)
        self.assertIn('duck_active{role="vm1",member="t3",host="t3-vm1"} 1', txt)
        for line in txt.splitlines():
            if line and not line.startswith("#"):
                self.assertRegex(line, r'^[a-z0-9_]+(\{.*\})? -?[\d.e+]+$')


if __name__ == "__main__":
    unittest.main(verbosity=2)

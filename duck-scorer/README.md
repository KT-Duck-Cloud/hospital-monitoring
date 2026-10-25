# duck-scorer — 점수 계산기 · DR-Control 연동

운영 멤버에 장애가 나면 **점수 1등 대기 노드**를 골라 DR-Control에 **target 메시지**를 보내는 프로그램입니다.
감시 서버(mon)에서 systemd 서비스로 돌아가며, 계산은 Python 표준 라이브러리, 조회 API는 FastAPI로 되어 있습니다.

```
Prometheus (수집된 지표) ─┐
tailscale ping ───────────┼─▶ duck-scorer ──(POST target JSON)──▶ DR-Control ──▶ 라우팅 변경 · Argo CD
TCP 6443 (K8s API) ───────┘     10초마다 판단
```

> 이 문서의 **「DR-Control 연동 명세」**는 DR-Control 관리자가 받는 기능을 만들 때 기준으로 쓰는 부분입니다.

---

## DR-Control 연동 API

### 1. 보내는 방식

| 항목 | 값 |
|---|---|
| 주소 | `https://duckcloud-admin.ktci5.kr/api/gateways` |
| 방식 | `POST`, `Content-Type: application/json` |
| 보내는 쪽 표시 | `User-Agent: duck-scorer/2.1` |
| 인증 | 현재 없음. 토큰을 쓰면 `Authorization: Bearer <토큰>` 헤더로 보냄 (토큰은 저장소에 올리지 않음) |
| 응답 대기 | 최대 10초 |

### 2. 메시지 형식 (고정)

```json
{
  "target":   {"site": "on-prem", "ip": "100.64.0.51", "host": "t5-vm1"},
  "failed":   {"ip": "100.64.0.31", "host": "t3-vm1"},
  "timestamp": "2026-10-25T10:00:00+09:00"
}
```

| 필드 | 설명 |
|---|---|
| `target.site` | `on-prem` 또는 `aws` |
| `target.ip`, `target.host` | 넘겨받을 VM. 장애 난 VM과 **같은 역할**(VM-1 ↔ VM-1, VM-2 ↔ VM-2) |
| `failed.ip`, `failed.host` | 장애가 난 VM |
| `timestamp` | 판정 시각, 한국 시간(`+09:00`) |

보내기 전에 Pydantic으로 형식을 검사하며, 맞지 않으면 **보내지 않습니다.**

### 3. 언제 보내나

| 장애 | 판정 기준 | 보내는 역할 |
|---|---|---|
| VM 꺼짐 (node-exporter 응답 없음) | 60초 연속 | 그 VM의 역할 |
| K8s API(6443) 응답 없음 | 60초 연속 | VM-1 |
| 앱(예약·인증·통계) **하나라도** `/readyz` 실패 | 60초 연속 | VM-1 |
| DB(`mysql_up`) 응답 없음 | **20초** 연속 | VM-2 |

- 10초마다 판단하므로, 기준 시간 뒤 **0~10초 안에** 전송됩니다.
- VM-1과 VM-2가 함께 장애면 **메시지 2개**가 따로 갑니다.
- 앱이 **503**(앱은 떴지만 DB 접속 실패)이면 앱 규칙이 아니라 **DB 규칙(20초)**이 처리합니다.

### 4. target 선택 규칙

1. **10분 이상 연속 정상**인 대기 멤버만 후보
2. VM-1 역할은 K8s API가 살아 있고 **앱이 모두 준비된** 멤버만, VM-2 역할은 **DB가 살아 있는** 멤버만
3. **온프레미스 우선**: 자격 있는 온프레미스 후보가 있으면 AWS는 고르지 않음
4. 그중 **점수 1등** (아래 점수표)

### 5. 보내지 않는 경우

| 상황 | 동작 |
|---|---|
| 자격 있는 후보 없음 | 보내지 않고 경고 로그 |
| Prometheus 조회 실패 | 그 회차는 판단 보류 |
| mysqld_exporter만 응답 없음 (VM은 정상) | DB 장애로 보지 않고 보류 |
| 이미 성공적으로 보낸 장애 | 그 VM이 회복될 때까지 다시 보내지 않음 |
| `send.enabled: false` (dry-run) | 판단만 하고 로그에 `[dry_run]`으로 기록 |

### 6. 응답 판단

| DR-Control 응답 | duck-scorer 판단 |
|---|---|
| **200번대** + 처리 결과 본문 (예: `{"accepted": true}`) | **성공** → 운영 노드 기록을 target으로 변경 |
| 200번대인데 본문이 **주소 안내문**(`gateways`, `public_domains` 포함) | 처리되지 않은 것으로 보고 **실패** |
| 그 밖의 응답 (4xx, 5xx, 연결 실패, 10초 초과) | **실패** |

실패하면 2초 간격으로 3번 다시 보내고, 그래도 실패하면 **60초 뒤** 같은 방식으로 다시 시도합니다(성공할 때까지).

### 7. DR-Control 쪽에 요청하는 것

- [ ] 메시지를 받아 처리했으면 **안내문이 아닌 응답**을 돌려주기 (예: `200` + `{"accepted": true}`)
- [ ] 재시도 때문에 **같은 메시지가 두 번 이상** 올 수 있으니 중복 처리 방식 정하기 (`failed.host` + `timestamp`로 구분 가능)
- [ ] 형식이 잘못된 요청에는 **4xx** 응답 (duck-scorer가 재시도를 멈출 수 있게)
- [ ] 연동 시험 방법 정하기 (시험 모드 또는 시험 시간)
- [ ] 인증이 필요하면 토큰 전달 (DM으로)

---

## 점수표 (최대 110점)

| 항목 | 배점 | 확인 방법 |
|---|---|---|
| 기본 | 40 | VM-1 · VM-2 모두 온라인 |
| 연속 안정 | 최대 30 | 1분에 0.5점, 60분이면 만점 |
| 직결 경로 | 10 | tailscale ping이 중계(DERP) 없이 직결 |
| K8s API | 10 | VM-1 6443 응답 |
| DB | 10 | `mysql_up == 1` |
| 가용 메모리 | 최대 10 | 두 VM 합계 8GB 이상이면 만점 |
| 지연 감점 | −10 | 왕복 50ms 초과 |

---

## 조회 API (mon 안에서만 접근)

`listen` 주소(`172.18.0.1:9200`)는 감시 서버와 그 안의 컨테이너에서만 닿습니다. 모두 **읽기 전용**이며 전환을 일으키지 않습니다.

| 주소 | 내용 |
|---|---|
| `GET /health` | 마지막 계산 시각, 기준값, 전송 켜짐 여부 (계산이 30초 넘게 멈추면 503) |
| `GET /scores` | 멤버별 점수, 후보 자격, 감점 이유, 준비 안 된 앱 |
| `GET /active` | 현재 운영 노드(vm1 · vm2)와 장애 여부, DB 판정 보류 여부 |
| `GET /target/vm1`, `/target/vm2` | 지금 장애가 나면 보낼 메시지 **미리보기** (보내지 않음) |
| `GET /metrics` | Prometheus 수집용 지표 (`duck_member_score` 등) |
| `GET /docs` | API 설명 화면 (자동 생성) |

```bash
curl -s http://172.18.0.1:9200/health | python3 -m json.tool
curl -s http://172.18.0.1:9200/target/vm1 | python3 -m json.tool
```

---

## 설정 (`/etc/duck-scorer/config.json`)

`config.example.json`을 복사해 씁니다. 주요 값:

| 키 | 현재 값 | 뜻 |
|---|---|---|
| `interval_sec` | 10 | 판단 주기 |
| `failover_sec` | 60 | VM · K8s API 장애 기준 |
| `db_failover_sec` | 20 | DB 장애 기준 |
| `app_failover_sec` | 60 | 앱 장애 기준 |
| `retry_sec` | 60 | 전송 실패 뒤 다시 보내기까지 |
| `stable_sec` | 600 | 후보가 되기 위한 연속 정상 시간 |
| `rtt_limit_ms` | 50 | 지연 감점 기준 |
| `prefer_onprem` | true | 온프레미스 우선 |
| `send.enabled` | false | **true여야 실제로 전송** (false = dry-run) |
| `send.url` | DR-Control 주소 | |
| `send.token_file` | "" | 토큰 파일 경로 (토큰 자체는 파일에만) |

---

## 설치 (감시 서버)

```bash
# 1. 가상환경 + 패키지
sudo apt install -y python3.10-venv
sudo python3 -m venv /opt/duck-scorer/venv
sudo /opt/duck-scorer/venv/bin/pip install -r requirements.txt

# 2. 파일 배치
sudo cp scorer.py api.py /opt/duck-scorer/
sudo mkdir -p /etc/duck-scorer && sudo cp config.example.json /etc/duck-scorer/config.json
sudo cp duck-scorer.service /etc/systemd/system/

# 3. 실행
sudo systemctl daemon-reload
sudo systemctl enable --now duck-scorer
journalctl -u duck-scorer -f
```

서비스는 `duck` 계정으로 실행되며, 상태 기록은 `/var/lib/duck-scorer/state.json`(systemd `StateDirectory`)에 남아 재시작해도 이어집니다.

## 시험

가짜 Prometheus · ping · 6443으로 26개 상황을 시험합니다 (장애 판정, 후보 선택, 온프레미스 우선, DB 20초, exporter 보류, 앱 장애, 안내문 응답 재시도 등).

```bash
python3 test_scorer.py
```

## 로그 읽는 법

```bash
journalctl -u duck-scorer --since today --no-pager | grep -E "\[전환\]|\[전송\]|\[경고\]|\[보류\]"
```

| 로그 | 뜻 |
|---|---|
| `[전환] ... [dry_run]` | 판단만 하고 보내지 않음 |
| `[전환] ... [sent] 성공` + `[전송] 응답 200: ...` | 보냈고 DR-Control이 처리함 |
| `[경고] 응답이 주소 안내문이에요` | 보냈지만 처리되지 않음 → 60초 뒤 재시도 |
| `[경고] 전송 실패 n/3` | 연결 실패 |
| `[보류] ... exporter 응답 없음` | DB 판정 보류 |

## 파일

| 파일 | 내용 |
|---|---|
| `scorer.py` | 수집 · 점수 · 장애 판정 · 후보 선택 · 전송 |
| `api.py` | FastAPI 조회 API, 메시지 형식 검사, 실행 진입점 |
| `test_scorer.py` | 시험 26개 |
| `config.example.json` | 설정 예시 |
| `duck-scorer.service` | systemd 서비스 |
| `requirements.txt` | `fastapi`, `uvicorn` |

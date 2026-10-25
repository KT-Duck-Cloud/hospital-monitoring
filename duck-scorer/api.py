#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Duck Cloud 점수 계산기 — FastAPI 버전 (duck-scorer)

계산 규칙은 scorer.py 그대로 쓰고, 바깥에서 묻고 답하는 부분만 FastAPI로 연다.

  GET /health          계산기가 살아 있는지 (마지막 계산이 오래되면 503)
  GET /scores          멤버별 점수, 후보 자격, 감점 이유
  GET /active          지금 운영 중인 노드 (vm1 · vm2)와 장애 여부
  GET /target/{role}   지금 그 역할이 장애 나면 보낼 target 메시지 미리보기 (보내지 않음)
  GET /metrics         Prometheus 수집용 (이전과 같은 숫자)
  GET /docs            API 설명 화면 (자동 생성)

실행: python api.py /etc/duck-scorer/config.json
"""
import json
import os
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Literal, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, ConfigDict, Field

from scorer import KST, Probe, Scorer, log


# ------------------------------------------------------------------ 메시지 형식 (DR-Control과 합의한 그대로)
class Target(BaseModel):
    model_config = ConfigDict(extra="forbid")
    site: Literal["on-prem", "aws"]
    ip: str = Field(pattern=r"^\d{1,3}(\.\d{1,3}){3}$")
    host: str = Field(min_length=1)


class Failed(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ip: str = Field(pattern=r"^\d{1,3}(\.\d{1,3}){3}$")
    host: str = Field(min_length=1)


class TargetMessage(BaseModel):
    """DR-Control에 보내는 target 메시지. 이 형식에 맞지 않으면 보내지 않는다."""
    model_config = ConfigDict(extra="forbid")
    target: Target
    failed: Failed
    timestamp: str = Field(pattern=r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+09:00$")


# ------------------------------------------------------------------ 응답 형식 (/docs에 그대로 표시됨)
class Parts(BaseModel):
    base: float
    stable: float
    direct: float
    k8s: float
    db: float
    mem: float
    latency: float


class MemberScore(BaseModel):
    site: str
    score: float = Field(description="건전도 점수 (최대 110)")
    eligible: bool = Field(description="후보 자격 (정상 상태 10분 이상)")
    healthy: bool
    stable_min: int = Field(description="연속 정상 시간 (분)")
    demoted: bool
    parts: Parts
    why: List[str] = Field(description="감점 · 대기 이유")
    app_down: List[str] = Field(default_factory=list, description="준비 안 된 앱 (있으면 VM-1 후보에서 제외)")


class ActiveVM(BaseModel):
    member: str
    host: str
    ip: str
    failed: bool
    reason: Optional[str] = None
    db_check_hold: bool = Field(False, description="mysqld_exporter 응답이 없어 DB 판정을 보류 중")


class Preview(BaseModel):
    role: Literal["vm1", "vm2"]
    candidate: str
    score: float
    message: TargetMessage
    note: str = "미리보기 — DR-Control로 보내지 않음"


class Health(BaseModel):
    status: Literal["ok", "stale"]
    last_run: Optional[str]
    age_sec: Optional[float]
    interval_sec: int
    failover_sec: int
    db_failover_sec: int
    app_failover_sec: int
    send_enabled: bool
    send_url: str
    prefer_onprem: bool


# ------------------------------------------------------------------ 앱
def create_app(scorer: Scorer, cfg: dict, run_loop: bool = True) -> FastAPI:
    stop = threading.Event()

    def checked_send(msg):
        """형식을 검사한 뒤에만 보낸다. 틀리면 보내지 않고 실패로 기록."""
        try:
            TargetMessage.model_validate(msg)
        except Exception as e:
            log(f"[오류] target 메시지 형식이 맞지 않아 보내지 않음: {e}")
            return False, "invalid"
        return scorer._http_send(msg)

    scorer.sender = checked_send

    def loop():
        log(f"시작 — {cfg['interval_sec']}초마다 계산, 장애 기준 VM·K8s {cfg['failover_sec']}초 / "
            f"DB {cfg.get('db_failover_sec', cfg['failover_sec'])}초 / 앱 {cfg.get('app_failover_sec', cfg['failover_sec'])}초, "
            f"전송 {'켜짐' if cfg['send'].get('enabled') else '꺼짐(dry-run)'}, "
            f"온프레미스 우선 {'켜짐' if cfg.get('prefer_onprem', True) else '꺼짐'}")
        while not stop.is_set():
            t0 = time.time()
            try:
                scorer.run_once()
                for n, r in scorer.last.get("results", {}).items():
                    log(f"  {n:4} {r['score']:6.1f}점  후보={'O' if r['eligible'] else 'X'}  "
                        f"{' · '.join(r['why']) or '정상'}")
            except Exception as e:
                log(f"[오류] {e!r}")
            stop.wait(max(1, cfg["interval_sec"] - (time.time() - t0)))

    @asynccontextmanager
    async def lifespan(app):
        th = None
        if run_loop:
            th = threading.Thread(target=loop, name="scorer-loop", daemon=True)
            th.start()
        yield
        stop.set()
        if th:
            th.join(timeout=5)

    app = FastAPI(
        title="Duck Cloud 점수 계산기",
        version="2.1",
        description="대기 노드 점수를 계산하고, 운영 노드 장애 시 1등 후보를 DR-Control에 target으로 보냅니다. "
                    "이 API는 **읽기 전용**이며 전환을 일으키지 않습니다.",
        lifespan=lifespan,
    )

    @app.get("/health", response_model=Health, tags=["상태"])
    def health():
        run = scorer.last_run
        age = time.time() - run if run else None
        stale = age is None or age > cfg["interval_sec"] * 3
        body = Health(
            status="stale" if stale else "ok",
            last_run=datetime.fromtimestamp(run, KST).isoformat(timespec="seconds") if run else None,
            age_sec=round(age, 1) if age is not None else None,
            interval_sec=cfg["interval_sec"],
            failover_sec=cfg["failover_sec"],
            db_failover_sec=cfg.get("db_failover_sec", cfg["failover_sec"]),
            app_failover_sec=cfg.get("app_failover_sec", cfg["failover_sec"]),
            send_enabled=bool(cfg["send"].get("enabled")),
            send_url=cfg["send"].get("url", ""),
            prefer_onprem=bool(cfg.get("prefer_onprem", True)),
        )
        return JSONResponse(body.model_dump(), status_code=503 if stale else 200)

    @app.get("/scores", response_model=Dict[str, MemberScore], tags=["점수"])
    def scores():
        return scorer.scores()

    @app.get("/active", response_model=Dict[str, ActiveVM], tags=["점수"])
    def active():
        return scorer.active()

    @app.get("/target/{role}", response_model=Preview, tags=["점수"],
             responses={409: {"description": "자격 있는 후보 없음"}})
    def target(role: Literal["vm1", "vm2"]):
        p = scorer.preview(role)
        if p is None:
            raise HTTPException(status_code=409, detail="자격 있는 후보가 없음")
        return p

    @app.get("/metrics", response_class=PlainTextResponse, tags=["수집"], include_in_schema=False)
    def metrics():
        return PlainTextResponse(scorer.metrics(), media_type="text/plain; version=0.0.4; charset=utf-8")

    return app


def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "/etc/duck-scorer/config.json"
    with open(cfg_path) as f:
        cfg = json.load(f)
    state_dir = os.environ.get("STATE_DIRECTORY", "/var/lib/duck-scorer")
    os.makedirs(state_dir, exist_ok=True)
    scorer = Scorer(cfg, Probe(cfg["prometheus"]), os.path.join(state_dir, "state.json"))
    app = create_app(scorer, cfg)
    host, port = cfg["listen"].rsplit(":", 1)
    log(f"API 주소: http://{cfg['listen']}  (설명 화면 /docs)")
    uvicorn.run(app, host=host, port=int(port), access_log=False, log_level="warning")


if __name__ == "__main__":
    main()

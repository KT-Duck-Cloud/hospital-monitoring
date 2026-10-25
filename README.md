# hospital-monitoring

Duck Cloud 병원 DR 시스템의 **모니터링 파트** 저장소입니다.
장애를 알아채고(수집 · 알림), 어디로 넘길지 정해(점수제) DR-Control에 전달합니다.

| 하는 일 | 도구 |
|---|---|
| 모으기 | Prometheus가 각 VM · DB · K8s · 앱 지표를 수집 (Headscale 사설망, 수집 포트만 허용) |
| 알리기 | Grafana Alerting → Discord (수집 실패와 서비스 장애를 다른 알림으로 구분) |
| 정하기 | **duck-scorer**: 대기 노드 점수 계산, 장애 시 1등을 DR-Control에 target으로 전송 |

## 폴더

| 폴더 | 내용 |
|---|---|
| [`duck-scorer/`](duck-scorer/) | 점수 계산기 · **DR-Control 연동 명세** |

## 비밀값

토큰 · 비밀번호 · 웹훅 주소는 이 저장소에 올리지 않습니다. 필요하면 담당자에게 DM으로 받습니다.

## 담당

모니터링 파트 김태현

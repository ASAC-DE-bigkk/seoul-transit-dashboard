# 서울 동네 교통 상태판 (Seoul Transit Dashboard)

서울 열린데이터광장 실시간 교통 Open API로 수집한 데이터를 **행정동 × 시간대**로 집계해
보여주는 단일 페이지 공개 대시보드입니다. 지도(choropleth)에서 주차 점유율 · 버스 혼잡도 ·
지하철 대기시간을 시간대별로 살펴볼 수 있습니다.

라이브 페이지는 저장소를 정적 호스팅(GitHub Pages 등)하거나, 로컬에서
`index.html` 을 열어 확인합니다.

## 구성

| 파일 | 설명 |
|------|------|
| `index.html` | 자체완결 단일 페이지 (외부 CDN 없음, vanilla JS + inline SVG). |
| `data/profile.js` | `window.PROFILE` — 동×시간대 지표(주차·버스·지하철) 일자 평균. |
| `data/boundary.js` | `window.BOUNDARY` — 서울 행정동 경계 GeoJSON(423동). |
| `scripts/export_data.py` | 데이터 재생성 스크립트 (Trino/Iceberg → 위 두 JS 파일). |

`data/*.js` 는 `fetch` 대신 전역 변수(`window.PROFILE`, `window.BOUNDARY`)로 주입되므로
`file://` 로 직접 열어도 동작합니다.

## 커버리지 (표본 안내)

이 대시보드는 서울 **전역**이 아니라 수집 대상 **축(axis)** 의 상태를 보여줍니다.

- 버스: 간선 5개 노선
- 지하철: 강남 · 잠실 · 사당 3역
- 공영주차장: 123개소

데이터가 없는 동은 지도에서 중립 회색으로 표시됩니다.

## 데이터 출처

- 서울 열린데이터광장 Open API (인증키 발급 활용)
  - 지하철 실시간 도착 정보
  - 버스 실시간 위치 정보
  - 공영주차장 실시간 정보
- 원천 → Bronze/Silver/Gold(dbt on Trino/Iceberg) 파이프라인을 거쳐
  `gold_transit_dong_hourly` (동×시간 grain) 로 집계된 값을 사용합니다.
- 행정동 경계·명칭: 행정안전부 행정동 마스터 및 경계 seed.

## 데이터 갱신 방법

`scripts/export_data.py` 가 Trino(Iceberg 카탈로그)에서 조회해 `data/profile.js` 와
`data/boundary.js` 를 다시 생성합니다. 접속 정보는 환경변수로 전달합니다(시크릿 미포함).

```bash
pip install trino

export TRINO_HOST=trino          # 기본값: trino
export TRINO_PORT=8080           # 기본값: 8080
export TRINO_USER=dashboard      # 기본값: dashboard
export TRINO_CATALOG=iceberg_dev # 기본값: iceberg_dev
export TRINO_SCHEMA=dev_codingpoppy94
export OUT_DIR=data              # 기본값: data

python scripts/export_data.py
```

Trino 클라이언트가 Airflow 스케줄러 컨테이너에만 있는 경우:

```bash
docker cp scripts/export_data.py elt-infra-airflow-scheduler-1:/tmp/
docker exec -e OUT_DIR=/tmp/out elt-infra-airflow-scheduler-1 \
    python /tmp/export_data.py
docker cp elt-infra-airflow-scheduler-1:/tmp/out/. data/
```

집계 규칙 요약:

- 각 `(행정동, 시각 0~23)` 조합에 대해 관측일들의 **평균**(값 없는 날은 자동 제외, null 유지).
- 지표: 버스 혼잡도(3 여유·4 보통·5 혼잡), 지하철 접근열차 잔여 도착시간(초),
  주차 점유율(현재대수÷총면수). 참고용으로 관측 건수 평균과 관측일수를 함께 저장.
- 경계 좌표는 파일 크기를 위해 소수 5자리로 반올림.

## 로컬 미리보기

```bash
python -m http.server 8000
# http://localhost:8000/ 접속
```

## 라이선스

이 저장소는 [MIT License](LICENSE) 하에 배포됩니다.
데이터 출처는 서울 열린데이터광장 Open API 이며, 각 데이터셋의 이용 조건을 따릅니다.

---
ASAC 데이터엔지니어링 팀

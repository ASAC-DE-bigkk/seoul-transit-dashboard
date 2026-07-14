#!/usr/bin/env python3
"""Export dashboard data (profile.js, boundary.js) from Trino / Iceberg.

Reads the gold transit model and admin-dong dimensions, aggregates to
(dong x hour) day-averages, and writes two self-contained JS files that the
static ``index.html`` loads via ``<script>`` (so it works over ``file://`` too).

Outputs
-------
data/profile.js   -> ``window.PROFILE = {...}``  (metrics per dong x hour)
data/boundary.js  -> ``window.BOUNDARY = {...}``  (admin-dong GeoJSON polygons)

Connection is configured entirely through environment variables so no secret is
committed:

    TRINO_HOST      (default: trino)
    TRINO_PORT      (default: 8080)
    TRINO_USER      (default: dashboard)
    TRINO_CATALOG   (default: iceberg_dev)
    TRINO_SCHEMA    (default: dev_codingpoppy94)
    OUT_DIR         (default: data)

Usage (from a host with the trino python client and network access to Trino):

    pip install trino
    OUT_DIR=data python scripts/export_data.py

In the reference dev stack the trino client lives in the Airflow scheduler
container, so it is run there and the results copied out:

    docker cp scripts/export_data.py elt-infra-airflow-scheduler-1:/tmp/
    docker exec -e OUT_DIR=/tmp/out elt-infra-airflow-scheduler-1 \
        python /tmp/export_data.py
    docker cp elt-infra-airflow-scheduler-1:/tmp/out/. data/
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import trino

KST = timezone(timedelta(hours=9))


def connect():
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "dashboard"),
        catalog=os.environ.get("TRINO_CATALOG", "iceberg_dev"),
        schema=os.environ.get("TRINO_SCHEMA", "dev_codingpoppy94"),
    )


def rows(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def r3(v):
    """Round to 3 decimals, preserving null (None stays None)."""
    return None if v is None else round(float(v), 3)


# 서울 실시간 도착 API subwayId -> 노선 배지명. 미매핑은 train_line_nm 로 폴백.
LINE_NM = {
    "1001": "1호선", "1002": "2호선", "1003": "3호선", "1004": "4호선",
    "1005": "5호선", "1006": "6호선", "1007": "7호선", "1008": "8호선",
    "1009": "9호선", "1063": "경의중앙선", "1065": "공항철도", "1067": "경춘선",
    "1075": "수인분당선", "1077": "신분당선", "1092": "우이신설선", "1093": "서해선",
}
# 수집 대상 역 표시 순서(커버리지 문구와 동일).
STATION_ORDER = ["강남", "잠실", "사당"]


def fmt_kst(dt):
    """event_at(naive KST 벽시계) -> 'YYYY-MM-DD HH:MM'."""
    return dt.strftime("%Y-%m-%d %H:%M") if hasattr(dt, "strftime") else str(dt)[:16]


def build_arrivals(cur):
    """지하철 실시간 도착정보의 '열차 잔여 도착시간' 대표 스냅샷.

    ARRIVALS_SNAPSHOT_KST(기본 2026-07-13 18:25, 월요일 저녁 러시) 이전의 가장
    최근 수집 run(dag_run_id) 한 벌을 골라 역×노선×방향별 도착 리스트로 편성한다.
    barvl_dt_sec(잔여초)·arvl_msg2(도착 메시지)를 그대로 실어 활용사례 심사에서
    실시간 도착정보 API 사용이 화면으로 확인되게 한다.
    """
    target = os.environ.get("ARRIVALS_SNAPSHOT_KST", "2026-07-13 18:25:00")
    data = rows(cur, """
        with pick as (
            select dag_run_id
            from slv_transit_subway_arrival
            where event_at <= timestamp '%s'
              and event_at >= timestamp '%s' - interval '90' minute
            order by event_at desc
            limit 1
        )
        select a.statn_nm, a.subway_id, a.train_line_nm, a.updn_line,
               a.barvl_dt_sec, a.arvl_msg2, a.terminal_statn_nm, a.is_last_train,
               a.event_at
        from slv_transit_subway_arrival a
        join pick p on a.dag_run_id = p.dag_run_id
        order by a.statn_nm, a.subway_id, a.updn_line, a.barvl_dt_sec
    """ % (target, target))

    stations, snap_max = {}, None
    for (statn, sid, service, updn, eta, msg, dest, last, ev) in data:
        s = stations.get(statn)
        if s is None:
            s = stations[statn] = {"name": statn, "received": None, "arrivals": []}
        s["arrivals"].append({
            "line": LINE_NM.get(sid, ""),
            "service": service or "",
            "updn": updn or "",
            "eta_s": None if eta is None else int(eta),
            "msg": msg or "",
            "dest": dest or "",
            "last": bool(last),
        })
        evs = fmt_kst(ev)
        if s["received"] is None or evs > s["received"]:
            s["received"] = evs
        if snap_max is None or evs > snap_max:
            snap_max = evs

    def order(name):
        return (STATION_ORDER.index(name) if name in STATION_ORDER else 99, name)

    return {
        "source": "서울 열린데이터광장 · 지하철 실시간 도착정보(realtimeStationArrival)",
        # 대표 스냅샷 수집시각(KST 벽시계). 정직 표기 — 실시간 라이브가 아닌 스냅샷.
        "snapshot_kst": snap_max or target[:16],
        "stations": [stations[k] for k in sorted(stations, key=order)],
    }


def build_profile(cur):
    # Day-average per (dong, hour-of-day). avg()/count() ignore NULLs, so a
    # dong x hour with no observation for a metric stays null (kept as-is), and
    # the day count is the number of distinct days that actually had a value.
    agg = rows(cur, """
        select
            admin_dong_code,
            hour(hour_at)                     as h,
            avg(bus_congestion_avg)           as bus,
            avg(subway_wait_avg_s)            as subway,
            avg(parking_occupancy_avg)        as parking,
            count(bus_congestion_avg)         as bus_days,
            count(subway_wait_avg_s)          as subway_days,
            count(parking_occupancy_avg)      as parking_days
        from gold_transit_dong_hourly
        where admin_dong_code is not null
        group by admin_dong_code, hour(hour_at)
    """)

    # dong name / gu lookup
    dim = {
        code: {"name": name, "gu": gu}
        for code, name, gu in rows(cur, """
            select admin_dong_code, admin_dong, gu from dim_admin_dong
        """)
    }

    meta = rows(cur, """
        select min(date(hour_at)), max(date(hour_at)),
               count(distinct date(hour_at))
        from gold_transit_dong_hourly
    """)[0]
    range_start, range_end, day_cnt = str(meta[0]), str(meta[1]), meta[2]

    dongs = {}
    for (code, h, bus, subway, parking,
         bus_days, subway_days, parking_days) in agg:
        d = dongs.get(code)
        if d is None:
            info = dim.get(code, {"name": code, "gu": ""})
            d = dongs[code] = {
                "name": info["name"],
                "gu": info["gu"],
                # metric day-averages
                "m": {"parking": [None] * 24, "bus": [None] * 24, "subway": [None] * 24},
                # observation-day count backing each average (for tooltips)
                "obs": {"parking": [0] * 24, "bus": [0] * 24, "subway": [0] * 24},
            }
        h = int(h)
        d["m"]["bus"][h] = r3(bus)
        d["m"]["subway"][h] = r3(subway)
        d["m"]["parking"][h] = r3(parking)
        d["obs"]["bus"][h] = int(bus_days)
        d["obs"]["subway"][h] = int(subway_days)
        d["obs"]["parking"][h] = int(parking_days)

    return {
        # generated = 이 export 를 실행한 시각(KST) — 페이지의 '마지막 갱신' 표기용.
        # (데이터 자체의 기간은 range 가 말한다. generated=range_end 로 두면
        #  스냅샷이 오래돼도 최신처럼 보이는 오표기가 되므로 분리.)
        "generated": datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        "range": [range_start, range_end],
        "days": day_cnt,
        # 재실행 diff 안정성: 동 코드 오름차순으로 직렬화.
        "dongs": {code: dongs[code] for code in sorted(dongs)},
    }


def round_coords(obj, nd=5):
    """Recursively round every coordinate number in a GeoJSON geometry."""
    if isinstance(obj, list):
        if obj and isinstance(obj[0], (int, float)):
            return [round(float(x), nd) for x in obj]
        return [round_coords(x, nd) for x in obj]
    return obj


def build_boundary(cur):
    # admin_dong_code null(경계 seed 에 코드 미부착) 행은 지도에서 색을 칠 수도,
    # 툴팁/선택 키로 쓸 수도 없으므로 제외한다. 코드 정렬은 재실행 diff 안정성용.
    data = rows(cur, """
        select
            admin_dong_code,
            dong,
            sigungu,
            to_geojson_geometry(ST_GeometryFromText(boundary_wkt)) as gj
        from seoul_admin_dong_boundary
        where boundary_wkt is not null
          and admin_dong_code is not null
        order by admin_dong_code
    """)
    features = []
    for code, name, gu, gj in data:
        geom = json.loads(gj)
        geom["coordinates"] = round_coords(geom["coordinates"], 5)
        features.append({
            "type": "Feature",
            "properties": {"code": code, "name": name, "gu": gu},
            "geometry": geom,
        })
    return {"type": "FeatureCollection", "features": features}


def write_js(path, varname, payload):
    with open(path, "w", encoding="utf-8") as f:
        f.write("window.%s = " % varname)
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        f.write(";\n")
    return os.path.getsize(path)


def main():
    out_dir = os.environ.get("OUT_DIR", "data")
    os.makedirs(out_dir, exist_ok=True)

    conn = connect()
    cur = conn.cursor()

    # 도착 전광판 스냅샷만 재생성(집계 profile/boundary 는 손대지 않음).
    if os.environ.get("ARRIVALS_ONLY") == "1":
        arrivals = build_arrivals(cur)
        a_path = os.path.join(out_dir, "arrivals.js")
        a_size = write_js(a_path, "ARRIVALS", arrivals)
        n_trains = sum(len(s["arrivals"]) for s in arrivals["stations"])
        print("arrivals.js: %d stations, %d trains, snapshot %s, %.1f KB" % (
            len(arrivals["stations"]), n_trains, arrivals["snapshot_kst"], a_size / 1024))
        return

    profile = build_profile(cur)
    boundary = build_boundary(cur)
    arrivals = build_arrivals(cur)

    p_path = os.path.join(out_dir, "profile.js")
    b_path = os.path.join(out_dir, "boundary.js")
    a_path = os.path.join(out_dir, "arrivals.js")
    p_size = write_js(p_path, "PROFILE", profile)
    b_size = write_js(b_path, "BOUNDARY", boundary)
    a_size = write_js(a_path, "ARRIVALS", arrivals)

    print("profile.js : %d dongs, range %s, %d days, %.1f KB" % (
        len(profile["dongs"]), " ~ ".join(profile["range"]),
        profile["days"], p_size / 1024))
    print("boundary.js: %d features, %.1f KB" % (
        len(boundary["features"]), b_size / 1024))
    print("arrivals.js: %d stations, snapshot %s, %.1f KB" % (
        len(arrivals["stations"]), arrivals["snapshot_kst"], a_size / 1024))
    print("total      : %.1f KB" % ((p_size + b_size + a_size) / 1024))


if __name__ == "__main__":
    sys.exit(main())

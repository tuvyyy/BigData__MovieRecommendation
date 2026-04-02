"""Streamlit web UI for the Movie Recommendation System.

This UI intentionally keeps the product flow simple for demos:
1) Intro
2) Recommend
3) Rate
"""

from __future__ import annotations

import base64
import html
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import streamlit as st

PROJECT_ROOT = Path(__file__).parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from utils.config import load_app_config  # noqa: E402

APP_CONFIG = load_app_config()
API_CONFIG = APP_CONFIG.get("api", {})
UI_CONFIG = APP_CONFIG.get("ui", {})
APP_SECTION = APP_CONFIG.get("app", {})

API_BASE_URL = str(API_CONFIG.get("base_url", "http://127.0.0.1:8000")).rstrip("/")
REQUEST_TIMEOUT_SEC = int(UI_CONFIG.get("request_timeout_sec", 45))
POSTER_LOOKUP_ENABLED = bool(UI_CONFIG.get("online_poster_lookup", True))
POSTER_LOOKUP_TIMEOUT_SEC = int(UI_CONFIG.get("poster_lookup_timeout_sec", 8))

TOP_N_DEFAULT = int(APP_SECTION.get("top_n", 10))
TOP_N_MIN = int(UI_CONFIG.get("top_n_min", 5))
TOP_N_MAX = int(UI_CONFIG.get("top_n_max", 20))

DEFAULT_KNOWN_USER_ID = int(UI_CONFIG.get("existing_user_demo_id", 2))
DEFAULT_GUEST_USER_BASE = int(UI_CONFIG.get("new_user_demo_id", 999999))

APP_TITLE = str(UI_CONFIG.get("title", "Movie Recommendation Studio"))
APP_SUBTITLE = str(
    UI_CONFIG.get("subtitle", "Distributed Spark + ALS + Hybrid + Cold Start")
)

TITLE_YEAR_PATTERN = re.compile(r"\s*\(\d{4}\)\s*$")


def _inject_css() -> None:
    st.markdown(
        """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&family=Be+Vietnam+Pro:wght@400;500;600&display=swap');

            :root {
                --bg-a: #e2e8f0;
                --bg-b: #f8fafc;
                --bg-c: #dbeafe;
                --hero-a: #0f172a;
                --hero-b: #1d4ed8;
                --hero-c: #0369a1;
                --text-main: #0f172a;
                --text-soft: #334155;
                --chip-bg: #e0f2fe;
                --chip-border: #7dd3fc;
                --card-bg: rgba(255, 255, 255, 0.74);
                --card-border: rgba(255, 255, 255, 0.7);
            }

            html, body, [class*="css"] {
                font-family: "Be Vietnam Pro", "Segoe UI", sans-serif;
            }

            h1, h2, h3, h4 {
                font-family: "Manrope", "Be Vietnam Pro", sans-serif;
                color: var(--text-main);
            }

            .stApp {
                background: linear-gradient(145deg, var(--bg-a) 0%, var(--bg-b) 46%, var(--bg-c) 100%);
                min-height: 100vh;
            }

            .block-container {
                max-width: 1200px;
                padding-top: 0.9rem;
            }

            .hero {
                border-radius: 18px;
                padding: 1.3rem 1.4rem;
                color: #f8fafc;
                background: linear-gradient(126deg, var(--hero-a) 0%, var(--hero-b) 54%, var(--hero-c) 100%);
                box-shadow: 0 16px 38px rgba(15, 23, 42, 0.2);
                margin-bottom: 0.8rem;
            }

            .hero h1 {
                margin: 0 0 0.2rem 0;
                font-size: 2.05rem;
                letter-spacing: -0.02em;
                color: #f8fafc;
            }

            .hero p {
                margin: 0;
                opacity: 0.94;
                font-size: 0.99rem;
            }

            .purpose {
                border: 1px solid var(--card-border);
                border-radius: 14px;
                background: var(--card-bg);
                backdrop-filter: blur(10px);
                padding: 0.95rem 1rem;
                margin-bottom: 0.9rem;
            }

            .step-item {
                border-left: 4px solid #38bdf8;
                padding-left: 0.65rem;
                margin-bottom: 0.5rem;
                color: var(--text-soft);
                font-size: 0.94rem;
            }

            .kpi-card {
                border: 1px solid var(--card-border);
                border-radius: 14px;
                background: var(--card-bg);
                backdrop-filter: blur(9px);
                padding: 0.75rem 0.88rem;
                box-shadow: 0 3px 10px rgba(15, 23, 42, 0.08);
                margin-bottom: 0.55rem;
            }

            .kpi-label {
                text-transform: uppercase;
                font-size: 0.76rem;
                letter-spacing: 0.04em;
                color: var(--text-soft);
                margin-bottom: 0.2rem;
            }

            .kpi-value {
                font-size: 1.12rem;
                font-weight: 800;
                color: var(--text-main);
            }

            .route-chip {
                display: inline-block;
                padding: 0.23rem 0.55rem;
                border: 1px solid var(--chip-border);
                border-radius: 999px;
                font-size: 0.78rem;
                font-weight: 700;
                background: var(--chip-bg);
                color: #0c4a6e;
                margin-right: 0.4rem;
                margin-bottom: 0.4rem;
            }

            .movie-card {
                border: 1px solid var(--card-border);
                border-radius: 16px;
                overflow: hidden;
                background: var(--card-bg);
                box-shadow: 0 8px 20px rgba(15, 23, 42, 0.1);
                margin-bottom: 0.95rem;
            }

            .movie-poster {
                width: 100%;
                height: 280px;
                object-fit: cover;
                display: block;
                background: #0f172a;
            }

            .movie-body {
                padding: 0.74rem 0.8rem 0.85rem 0.8rem;
            }

            .movie-rank {
                display: inline-block;
                font-size: 0.74rem;
                font-weight: 700;
                border-radius: 999px;
                padding: 0.12rem 0.48rem;
                background: #e2e8f0;
                color: #0f172a;
                margin-bottom: 0.34rem;
            }

            .movie-title {
                font-size: 0.99rem;
                font-weight: 700;
                color: #0f172a;
                margin-bottom: 0.22rem;
                line-height: 1.28;
                min-height: 2.5em;
            }

            .movie-meta {
                font-size: 0.85rem;
                color: #334155;
                margin-bottom: 0.12rem;
            }

            .movie-explain {
                font-size: 0.8rem;
                color: #1e3a5f;
                margin-top: 0.34rem;
                line-height: 1.32;
                min-height: 3.4em;
            }

            .stButton > button {
                transition: all 0.2s ease;
            }

            .stButton > button:hover {
                transform: translateY(-1px);
                box-shadow: 0 8px 15px rgba(30, 64, 175, 0.16);
            }

            .flow-card {
                border: 1px solid var(--card-border);
                border-radius: 12px;
                background: var(--card-bg);
                padding: 0.7rem 0.8rem;
                margin-bottom: 0.5rem;
            }

            .flow-title {
                font-size: 0.9rem;
                font-weight: 700;
                color: #0f172a;
                margin-bottom: 0.2rem;
            }

            .flow-status {
                font-size: 0.82rem;
                color: #334155;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _init_state() -> None:
    if "personalize_mode" not in st.session_state:
        st.session_state["personalize_mode"] = False
    if "known_user_id" not in st.session_state:
        st.session_state["known_user_id"] = DEFAULT_KNOWN_USER_ID
    if "guest_user_id" not in st.session_state:
        st.session_state["guest_user_id"] = DEFAULT_GUEST_USER_BASE + random.randint(1000, 99999)
    if "top_n" not in st.session_state:
        st.session_state["top_n"] = TOP_N_DEFAULT
    if "genre" not in st.session_state:
        st.session_state["genre"] = ""
    if "health_payload" not in st.session_state:
        st.session_state["health_payload"] = {}
    if "recommend_payload" not in st.session_state:
        st.session_state["recommend_payload"] = {}
    if "latest_latency_ms" not in st.session_state:
        st.session_state["latest_latency_ms"] = None
    if "selected_movie_id" not in st.session_state:
        st.session_state["selected_movie_id"] = None
    if "rate_response" not in st.session_state:
        st.session_state["rate_response"] = None


def _api_get(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{API_BASE_URL}{endpoint}"
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
    if response.status_code >= 400:
        raise RuntimeError(f"{response.status_code} {response.reason}: {response.text[:300]}")
    return response.json()


def _api_post(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE_URL}{endpoint}"
    response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SEC)
    if response.status_code >= 400:
        raise RuntimeError(f"{response.status_code} {response.reason}: {response.text[:300]}")
    return response.json()


def _load_health(force: bool = False) -> None:
    if st.session_state.get("health_payload") and not force:
        return
    try:
        st.session_state["health_payload"] = _api_get("/health")
    except Exception as exc:  # pylint: disable=broad-except
        st.session_state["health_payload"] = {"status": f"api_unreachable: {exc}"}


def _active_user_id() -> int:
    if bool(st.session_state.get("personalize_mode", False)):
        return int(st.session_state.get("known_user_id", DEFAULT_KNOWN_USER_ID))
    return int(st.session_state.get("guest_user_id", DEFAULT_GUEST_USER_BASE))


def _fetch_recommendations() -> None:
    user_id = _active_user_id()
    top_n = int(st.session_state["top_n"])
    genre = str(st.session_state.get("genre", "")).strip()

    query: Dict[str, Any] = {"top_n": top_n}
    if genre:
        query["genre"] = genre

    with st.spinner("Dang lay goi y..."):
        try:
            start = time.perf_counter()
            payload = _api_get(f"/recommend/{user_id}", params=query)
            st.session_state["latest_latency_ms"] = (time.perf_counter() - start) * 1000
            st.session_state["recommend_payload"] = payload
            rec_rows = payload.get("recommendations", [])
            st.session_state["selected_movie_id"] = rec_rows[0].get("movieId") if rec_rows else None
        except Exception as exc:  # pylint: disable=broad-except
            st.session_state["recommend_payload"] = {"error": str(exc)}


def _has_recommendations() -> bool:
    payload = st.session_state.get("recommend_payload", {})
    if not payload or payload.get("error"):
        return False
    return bool(payload.get("recommendations"))


def _workflow_step() -> int:
    if st.session_state.get("rate_response"):
        return 3
    if _has_recommendations():
        return 2
    return 1


def _render_workflow_progress() -> None:
    step = _workflow_step()
    st.progress(step / 3)
    if step == 1:
        st.info("Buoc 1/3: Bam 'Lay goi y' de lay danh sach phim de xuat.")
    elif step == 2:
        st.info("Buoc 2/3: Da co ket qua. Xem danh sach phim goi y.")
    else:
        st.info("Buoc 3/3: Gui danh gia de cap nhat he thong.")


def _render_flow_cards() -> None:
    has_recs = _has_recommendations()
    st.markdown(
        """
        <div class='flow-card'>
          <div class='flow-title'>Buoc 1: Lay goi y</div>
          <div class='flow-status'>Nhan 1 nut de he thong de xuat phim phu hop.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div class='flow-card'>
          <div class='flow-title'>Buoc 2: Xem ket qua</div>
          <div class='flow-status'>{'Da co ket qua goi y.' if has_recs else 'Chua co ket qua, can chay Buoc 1.'}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div class='flow-card'>
          <div class='flow-title'>Buoc 3: Danh gia phim</div>
          <div class='flow-status'>{'San sang gui rating.' if has_recs else 'Nen xem goi y truoc de chon phim nhanh hon.'}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


@st.cache_data(show_spinner=False, ttl=86400, max_entries=1200)
def _lookup_itunes_poster(movie_title: str) -> Optional[str]:
    if not POSTER_LOOKUP_ENABLED:
        return None

    query_title = TITLE_YEAR_PATTERN.sub("", movie_title or "").strip()
    if not query_title:
        return None

    try:
        response = requests.get(
            "https://itunes.apple.com/search",
            params={"term": query_title, "media": "movie", "entity": "movie", "limit": 1},
            timeout=POSTER_LOOKUP_TIMEOUT_SEC,
        )
        if response.status_code != 200:
            return None
        rows = response.json().get("results", [])
        if not rows:
            return None
        poster = rows[0].get("artworkUrl100")
        if not poster:
            return None
        return poster.replace("100x100bb.jpg", "600x900bb.jpg").replace("100x100bb", "600x900bb")
    except Exception:  # pylint: disable=broad-except
        return None


def _svg_poster(title: str, movie_id: Any) -> str:
    safe_title = _xml_escape((title or "Unknown Movie")[:42])
    safe_id = _xml_escape(str(movie_id))
    svg = f"""
    <svg xmlns='http://www.w3.org/2000/svg' width='600' height='900'>
      <defs>
        <linearGradient id='bg' x1='0' y1='0' x2='1' y2='1'>
          <stop offset='0%' stop-color='#0f172a'/>
          <stop offset='55%' stop-color='#1d4ed8'/>
          <stop offset='100%' stop-color='#0369a1'/>
        </linearGradient>
      </defs>
      <rect width='100%' height='100%' fill='url(#bg)'/>
      <circle cx='510' cy='100' r='72' fill='rgba(255,255,255,0.14)'/>
      <rect x='44' y='610' width='512' height='220' rx='18' fill='rgba(15,23,42,0.35)'/>
      <text x='58' y='680' fill='#f8fafc' font-family='Segoe UI, Arial' font-size='34' font-weight='700'>{safe_title}</text>
      <text x='58' y='730' fill='#bae6fd' font-family='Segoe UI, Arial' font-size='25'>Movie ID: {safe_id}</text>
      <text x='58' y='775' fill='#e2e8f0' font-family='Segoe UI, Arial' font-size='22'>Recommendation Preview</text>
    </svg>
    """.strip()
    encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


def _poster_url(movie: Dict[str, Any]) -> str:
    title = str(movie.get("title", ""))
    movie_id = movie.get("movieId", "N/A")
    poster = _lookup_itunes_poster(title)
    if poster:
        return poster
    return _svg_poster(title=title, movie_id=movie_id)


def _render_kpi_cards(health_payload: Dict[str, Any]) -> None:
    metrics = health_payload.get("metrics", {})
    best_rmse = metrics.get("best_rmse")
    model_version = health_payload.get("model_version") or "N/A"
    model_ready = "SAN SANG" if health_payload.get("model_ready") else "CHUA SAN SANG"
    status = str(health_payload.get("status", "unknown"))

    cards = [
        ("API", status),
        ("Model", model_ready),
        ("Best RMSE", f"{float(best_rmse):.5f}" if best_rmse is not None else "N/A"),
        ("Version", model_version),
    ]

    cols = st.columns(4)
    for col, (label, value) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
                <div class='kpi-card'>
                  <div class='kpi-label'>{html.escape(label)}</div>
                  <div class='kpi-value'>{html.escape(str(value))}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def _render_hero() -> None:
    st.markdown(
        f"""
        <div class='hero'>
          <h1>{html.escape(APP_TITLE)}</h1>
          <p>{html.escape(APP_SUBTITLE)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_intro_page(health_payload: Dict[str, Any]) -> None:
    _render_hero()
    _render_kpi_cards(health_payload)
    _render_workflow_progress()

    st.markdown(
        """
        <div class='purpose'>
          <h3>He thong nay de lam gi?</h3>
          <p style='margin-top:0.2rem; color:#334155;'>
            Day la web goi y phim ca nhan hoa dua tren Spark + ALS + Hybrid.
            Neu ban chua co lich su, he thong van de xuat phim theo fallback.
          </p>
          <h4 style='margin-top:0.8rem;'>Cach dung nhanh</h4>
          <div class='step-item'>1. Bam "Lay goi y".</div>
          <div class='step-item'>2. Xem danh sach phim de xuat ben duoi.</div>
          <div class='step-item'>3. Chon phim va gui danh gia de cap nhat he thong.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _render_flow_cards()

    payload = st.session_state.get("recommend_payload", {})
    if payload and not payload.get("error"):
        st.markdown("### Lan goi y gan nhat")
        rows = payload.get("recommendations", [])[:5]
        st.dataframe(
            [
                {
                    "rank": idx + 1,
                    "movieId": row.get("movieId"),
                    "title": row.get("title"),
                    "genres": row.get("genres"),
                    "score": round(float(row.get("score", 0.0)), 4),
                }
                for idx, row in enumerate(rows)
            ],
            use_container_width=True,
            hide_index=True,
        )


def _render_recommend_page() -> None:
    st.markdown("## Ket qua goi y")
    _render_workflow_progress()
    payload = st.session_state.get("recommend_payload", {})

    if not payload:
        st.info("Chua co du lieu goi y. Hay bam 'Lay goi y' o sidebar.")
        return
    if payload.get("error"):
        st.error(f"Khong goi duoc API goi y: {payload['error']}")
        return

    route = str(payload.get("route", "unknown"))
    user_id = payload.get("user_id")
    top_n = payload.get("top_n")
    latency_ms = st.session_state.get("latest_latency_ms")

    st.markdown(
        f"<span class='route-chip'>route: {html.escape(route)}</span>"
        f"<span class='route-chip'>user: {html.escape(str(user_id))}</span>"
        f"<span class='route-chip'>top_n: {html.escape(str(top_n))}</span>",
        unsafe_allow_html=True,
    )
    if latency_ms is not None:
        st.caption(f"Do tre API: {latency_ms:.0f} ms")

    rows: List[Dict[str, Any]] = payload.get("recommendations", [])
    if not rows:
        st.warning("API da tra ve route nhung danh sach phim rong.")
        return

    columns = st.columns(3)
    for index, movie in enumerate(rows):
        col = columns[index % 3]
        with col:
            score = float(movie.get("score", 0.0))
            poster = _poster_url(movie)
            title = str(movie.get("title", "Unknown"))
            genres = str(movie.get("genres", "N/A"))
            explain = str(movie.get("explain", "")).strip() or "No explanation"
            movie_id = movie.get("movieId", "N/A")
            st.markdown(
                f"""
                <div class='movie-card'>
                  <img src='{html.escape(poster, quote=True)}' class='movie-poster' alt='{html.escape(title, quote=True)}' />
                  <div class='movie-body'>
                    <div class='movie-rank'>#{index + 1}</div>
                    <div class='movie-title'>{html.escape(title)}</div>
                    <div class='movie-meta'>The loai: {html.escape(genres)}</div>
                    <div class='movie-meta'>Score: {score:.4f}</div>
                    <div class='movie-meta'>Movie ID: {html.escape(str(movie_id))}</div>
                    <div class='movie-explain'>{html.escape(explain)}</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    movie_options = {
        f"{row.get('title', 'Unknown')} (id={row.get('movieId')})": int(row["movieId"])
        for row in rows
        if row.get("movieId") is not None
    }
    if movie_options:
        st.markdown("### Chon phim de danh gia")
        chosen_label = st.selectbox("Chon phim", options=list(movie_options.keys()), key="pick_for_rate")
        if st.button("Chon phim nay cho form danh gia", use_container_width=False):
            st.session_state["selected_movie_id"] = movie_options[chosen_label]
            st.success("Da chon phim cho form danh gia ben duoi.")


def _render_rate_page() -> None:
    st.markdown("## Danh gia phim")
    _render_workflow_progress()
    st.caption(f"Tai khoan hien tai: {_active_user_id()}")

    payload = st.session_state.get("recommend_payload", {})
    rows = payload.get("recommendations", []) if payload and not payload.get("error") else []
    movie_options: Dict[str, int] = {
        f"{row.get('title', 'Unknown')} (id={row.get('movieId')})": int(row["movieId"])
        for row in rows
        if row.get("movieId") is not None
    }

    with st.form("rating_form", clear_on_submit=False):
        st.markdown("Gui feedback ve API /rate")

        if movie_options:
            labels = list(movie_options.keys())
            default_id = st.session_state.get("selected_movie_id")
            default_index = 0
            if default_id is not None:
                for idx, label in enumerate(labels):
                    if movie_options[label] == int(default_id):
                        default_index = idx
                        break
            selected_label = st.selectbox("Chon phim tu ket qua goi y", options=labels, index=default_index)
            movie_id_value = movie_options[selected_label]
        else:
            movie_id_value = int(st.number_input("Movie ID", min_value=1, value=1, step=1))

        rating_choices = [step / 2 for step in range(1, 11)]
        rating_value = float(st.select_slider("Rating", options=rating_choices, value=4.0))
        retrain_flag = st.checkbox("Retrain batch sau khi gui", value=True)
        submitted = st.form_submit_button("Gui danh gia", use_container_width=True)

    if submitted:
        with st.spinner("Dang gui danh gia..."):
            try:
                response = _api_post(
                    "/rate",
                    {
                        "userId": _active_user_id(),
                        "movieId": int(movie_id_value),
                        "rating": float(rating_value),
                        "retrain": bool(retrain_flag),
                    },
                )
                st.session_state["rate_response"] = response
                st.success("Gui danh gia thanh cong.")
                _load_health(force=True)
            except Exception as exc:  # pylint: disable=broad-except
                st.error(f"Gui danh gia that bai: {exc}")

    if st.session_state.get("rate_response"):
        st.markdown("### Phan hoi tu API")
        st.json(st.session_state["rate_response"])


def main() -> None:
    st.set_page_config(page_title="Movie Recommendation Web", layout="wide", initial_sidebar_state="expanded")
    _inject_css()
    _init_state()

    with st.sidebar:
        st.markdown("## Movie Recommendation Web")
        st.caption("Nhan 1 nut de lay goi y, sau do danh gia phim ngay ben duoi.")
        st.markdown("---")

        st.toggle("Ca nhan hoa bang User ID", key="personalize_mode")
        if bool(st.session_state.get("personalize_mode", False)):
            st.number_input("User ID cua ban", min_value=1, step=1, key="known_user_id")
        else:
            st.caption(f"Che do khach: {st.session_state.get('guest_user_id')}")
            if st.button("Tao ma khach moi", use_container_width=True):
                st.session_state["guest_user_id"] = DEFAULT_GUEST_USER_BASE + random.randint(1000, 99999)
                st.session_state["recommend_payload"] = {}
                st.session_state["rate_response"] = None
                st.rerun()

        st.slider("Top-N", min_value=TOP_N_MIN, max_value=TOP_N_MAX, step=1, key="top_n")
        st.text_input("Genre (optional)", key="genre")

        fetch_clicked = st.button("Lay goi y cho toi", use_container_width=True, type="primary")
        refresh_clicked = st.button("Lam moi API health", use_container_width=True)
        st.caption(f"API: {API_BASE_URL}")

    _load_health(force=refresh_clicked)
    health_payload = st.session_state.get("health_payload", {})
    if "api_unreachable" in str(health_payload.get("status", "")):
        st.error(f"API chua san sang: {health_payload.get('status')}")

    if fetch_clicked:
        _fetch_recommendations()

    _render_intro_page(health_payload=health_payload)
    _render_recommend_page()
    _render_rate_page()


if __name__ == "__main__":
    main()

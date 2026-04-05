"""FastAPI backend for movie recommendations."""

from __future__ import annotations

import sys
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field

PROJECT_ROOT = Path(__file__).parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from model.feedback import retrain_with_feedback  # noqa: E402
from service.recommendation_service import RecommendationService  # noqa: E402
from utils.config import load_app_config  # noqa: E402
from utils.logger import get_logger  # noqa: E402
from serving.sql_store import SqlStore, verify_password  # noqa: E402

APP_CONFIG = load_app_config()
app = FastAPI(title="Movie Recommendation API", version="1.0.0")
SERVICE: RecommendationService | None = None
SQL_STORE: SqlStore | None = None
API_LOGGER = get_logger("api", log_file="logs/app.log")
WEB_DIST_DIR = PROJECT_ROOT / "web" / "dist"

cors_origins = APP_CONFIG.get("api", {}).get(
    "cors_origins",
    ["http://localhost:5173", "http://127.0.0.1:5173"],
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[str(origin) for origin in cors_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RateRequest(BaseModel):
    """Payload for feedback submission."""

    model_config = ConfigDict(populate_by_name=True)

    user_id: int = Field(alias="userId")
    movie_id: int = Field(alias="movieId")
    rating: float
    retrain: bool = True


class EventRequest(BaseModel):
    """Payload for behavior tracking."""

    model_config = ConfigDict(populate_by_name=True)

    event_type: str = Field(alias="eventType")
    user_id: int = Field(alias="userId")
    movie_id: Optional[int] = Field(default=None, alias="movieId")
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RegisterRequest(BaseModel):
    """Payload for account registration."""

    model_config = ConfigDict(populate_by_name=True)

    ten_tai_khoan: str = Field(alias="tenTaiKhoan")
    email: str
    mat_khau: str = Field(alias="matKhau")
    ho_ten: Optional[str] = Field(default=None, alias="hoTen")


class LoginRequest(BaseModel):
    """Payload for login."""

    model_config = ConfigDict(populate_by_name=True)

    ten_dang_nhap: str = Field(alias="tenDangNhap")
    mat_khau: str = Field(alias="matKhau")


class CreateProfileRequest(BaseModel):
    """Payload for creating profile under current account."""

    model_config = ConfigDict(populate_by_name=True)

    ten_ho_so: str = Field(alias="tenHoSo")
    che_do_goi_y: str = Field(default="so_thich_ban_dau", alias="cheDoGoiY")
    the_loai_uu_tien: Optional[str] = Field(default=None, alias="theLoaiUuTien")
    id_user_ml: Optional[int] = Field(default=None, alias="idUserMl")


class ProfileFeedbackRequest(BaseModel):
    """Payload for profile-based rating feedback."""

    model_config = ConfigDict(populate_by_name=True)

    id_ho_so: int = Field(alias="idHoSo")
    id_phim: int = Field(alias="idPhim")
    diem_danh_gia: float = Field(alias="diemDanhGia")
    retrain: bool = False


def _require_service() -> RecommendationService:
    if SERVICE is None:
        raise RuntimeError("Recommendation service is not initialized")
    return SERVICE


def _require_sql_store() -> SqlStore:
    if SQL_STORE is None:
        raise RuntimeError("SQL store is not initialized")
    return SQL_STORE


def _extract_token(
    authorization: Optional[str],
    x_auth_token: Optional[str],
) -> Optional[str]:
    if authorization:
        prefix = "Bearer "
        if authorization.startswith(prefix):
            return authorization[len(prefix) :].strip()
        return authorization.strip()
    if x_auth_token:
        return x_auth_token.strip()
    return None


def _require_authenticated_user(
    authorization: Optional[str],
    x_auth_token: Optional[str],
):
    token = _extract_token(authorization=authorization, x_auth_token=x_auth_token)
    if not token:
        raise HTTPException(status_code=401, detail="Thiếu token đăng nhập")
    user = _require_sql_store().resolve_session_user(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Phiên đăng nhập không hợp lệ hoặc đã hết hạn")
    return user, token


def _reset_service(reason: str) -> None:
    global SERVICE
    if SERVICE is not None:
        try:
            SERVICE.close()
        except Exception:  # pylint: disable=broad-except
            pass
    SERVICE = RecommendationService(config=APP_CONFIG)
    API_LOGGER.warning("event=service_reset reason=%s", reason)


def _is_recoverable_spark_assertion(exc: Exception) -> bool:
    if isinstance(exc, AssertionError):
        return True
    text = str(exc)
    return ("AssertionError" in text) or ("sc is not None" in text)


def _run_background_retrain() -> None:
    service = _require_service()
    retrain_with_feedback(service.config)
    service.invalidate_runtime_cache()


@app.on_event("startup")
def startup() -> None:
    global SERVICE, SQL_STORE
    SERVICE = RecommendationService(config=APP_CONFIG)
    database_cfg = APP_CONFIG.get("database", {})
    raw_db_path = str(database_cfg.get("path", "data/sql/recommendation.db"))
    db_path = Path(raw_db_path)
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    SQL_STORE = SqlStore(db_path=db_path)
    seed_result = SQL_STORE.seed_demo_account()
    API_LOGGER.info(
        "event=sql_store_ready db_path=%s seeded=%s demo_user=%s",
        db_path,
        seed_result.get("seeded"),
        seed_result.get("ten_tai_khoan"),
    )


@app.on_event("shutdown")
def shutdown() -> None:
    global SQL_STORE
    if SERVICE is not None:
        SERVICE.close()
    if SQL_STORE is not None:
        SQL_STORE.close()
        SQL_STORE = None


@app.get("/health")
def health() -> dict:
    """Health endpoint."""
    try:
        return _require_service().health()
    except Exception as exc:  # pylint: disable=broad-except
        if _is_recoverable_spark_assertion(exc):
            _reset_service(reason="health_assertion_recover")
            return _require_service().health()
        raise


@app.get("/recommend/{user_id}")
def recommend(
    user_id: int,
    top_n: int = 10,
    genre: Optional[str] = None,
) -> dict:
    """Recommendation endpoint."""
    try:
        return _require_service().recommend(user_id=user_id, top_n=top_n, preferred_genre=genre)
    except Exception as exc:  # pylint: disable=broad-except
        if _is_recoverable_spark_assertion(exc):
            try:
                _reset_service(reason="recommend_assertion_recover")
                return _require_service().recommend(user_id=user_id, top_n=top_n, preferred_genre=genre)
            except Exception as retry_exc:  # pylint: disable=broad-except
                exc = retry_exc
        detail = str(exc).strip() or f"{type(exc).__name__}: no detail message"
        API_LOGGER.error(
            "event=api_recommend_failed user_id=%s top_n=%s genre=%s detail=%s traceback=%s",
            user_id,
            top_n,
            genre,
            detail,
            traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=detail) from exc


@app.post("/rate")
def rate(payload: RateRequest, background_tasks: BackgroundTasks) -> dict:
    """Rating feedback endpoint."""
    if payload.rating < 0.5 or payload.rating > 5.0:
        raise HTTPException(status_code=400, detail="rating must be between 0.5 and 5.0")

    try:
        result = _require_service().add_rating(
            user_id=payload.user_id,
            movie_id=payload.movie_id,
            rating=payload.rating,
            trigger_retrain=False,
        )
        if payload.retrain:
            background_tasks.add_task(_run_background_retrain)
            result["retrain_status"] = "scheduled"
        else:
            result["retrain_status"] = "skipped"
        return result
    except Exception as exc:  # pylint: disable=broad-except
        detail = str(exc).strip() or f"{type(exc).__name__}: no detail message"
        API_LOGGER.error(
            "event=api_rate_failed user_id=%s movie_id=%s rating=%s detail=%s traceback=%s",
            payload.user_id,
            payload.movie_id,
            payload.rating,
            detail,
            traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=detail) from exc


@app.post("/event")
def event(payload: EventRequest) -> dict:
    """Behavior tracking endpoint: click/view/rate/skip."""
    try:
        return _require_service().add_event(
            event_type=payload.event_type,
            user_id=payload.user_id,
            movie_id=payload.movie_id,
            metadata=payload.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pylint: disable=broad-except
        detail = str(exc).strip() or f"{type(exc).__name__}: no detail message"
        API_LOGGER.error(
            "event=api_event_failed event_type=%s user_id=%s movie_id=%s detail=%s traceback=%s",
            payload.event_type,
            payload.user_id,
            payload.movie_id,
            detail,
            traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=detail) from exc


@app.post("/dang-ky")
def dang_ky(payload: RegisterRequest) -> dict:
    """Đăng ký tài khoản mới và tạo hồ sơ mặc định."""
    ten_tai_khoan = payload.ten_tai_khoan.strip()
    email = payload.email.strip().lower()
    if len(ten_tai_khoan) < 3:
        raise HTTPException(status_code=400, detail="Tên tài khoản cần ít nhất 3 ký tự")
    if "@" not in email:
        raise HTTPException(status_code=400, detail="Email không hợp lệ")
    if len(payload.mat_khau) < 6:
        raise HTTPException(status_code=400, detail="Mật khẩu cần ít nhất 6 ký tự")

    store = _require_sql_store()
    existing = store.get_user_by_username_or_email(ten_tai_khoan) or store.get_user_by_username_or_email(email)
    if existing:
        raise HTTPException(status_code=409, detail="Tài khoản hoặc email đã tồn tại")

    try:
        user_id = store.create_user(
            ten_tai_khoan=ten_tai_khoan,
            email=email,
            mat_khau_raw=payload.mat_khau,
            ho_ten=payload.ho_ten,
        )
        default_profile = store.create_profile(
            id_nguoi_dung=user_id,
            ten_ho_so="Hồ sơ chính",
            che_do_goi_y="so_thich_ban_dau",
            the_loai_uu_tien="Action",
            id_user_ml=None,
        )
        return {
            "status": "ok",
            "nguoi_dung": {
                "id_nguoi_dung": user_id,
                "ten_tai_khoan": ten_tai_khoan,
                "email": email,
                "ho_ten": payload.ho_ten,
            },
            "ho_so_mac_dinh": default_profile,
        }
    except Exception as exc:  # pylint: disable=broad-except
        detail = str(exc).strip() or f"{type(exc).__name__}: no detail message"
        API_LOGGER.error("event=api_register_failed detail=%s traceback=%s", detail, traceback.format_exc())
        raise HTTPException(status_code=500, detail=detail) from exc


@app.post("/dang-nhap")
def dang_nhap(payload: LoginRequest) -> dict:
    """Đăng nhập và trả token phiên."""
    store = _require_sql_store()
    user = store.get_user_by_username_or_email(payload.ten_dang_nhap.strip())
    if not user or not verify_password(payload.mat_khau, str(user["mat_khau"])):
        raise HTTPException(status_code=401, detail="Sai tên đăng nhập/email hoặc mật khẩu")

    token = store.create_session(id_nguoi_dung=int(user["id_nguoi_dung"]), ttl_hours=24)
    profiles = store.list_profiles(id_nguoi_dung=int(user["id_nguoi_dung"]))
    return {
        "status": "ok",
        "token": token,
        "nguoi_dung": {
            "id_nguoi_dung": int(user["id_nguoi_dung"]),
            "ten_tai_khoan": user["ten_tai_khoan"],
            "email": user["email"],
            "ho_ten": user["ho_ten"],
            "vai_tro": user["vai_tro"],
        },
        "ho_so": profiles,
    }


@app.get("/ho-so")
def lay_ho_so(
    authorization: Optional[str] = Header(default=None),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
) -> dict:
    """Lấy danh sách hồ sơ thuộc tài khoản đã đăng nhập."""
    user, _ = _require_authenticated_user(authorization=authorization, x_auth_token=x_auth_token)
    profiles = _require_sql_store().list_profiles(id_nguoi_dung=user.id_nguoi_dung)
    return {"status": "ok", "ho_so": profiles}


@app.post("/ho-so")
def tao_ho_so(
    payload: CreateProfileRequest,
    authorization: Optional[str] = Header(default=None),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
) -> dict:
    """Tạo hồ sơ mới cho user đã đăng nhập."""
    user, _ = _require_authenticated_user(authorization=authorization, x_auth_token=x_auth_token)
    if not payload.ten_ho_so.strip():
        raise HTTPException(status_code=400, detail="Tên hồ sơ không được để trống")

    profile = _require_sql_store().create_profile(
        id_nguoi_dung=user.id_nguoi_dung,
        ten_ho_so=payload.ten_ho_so.strip(),
        che_do_goi_y=payload.che_do_goi_y,
        the_loai_uu_tien=payload.the_loai_uu_tien,
        id_user_ml=payload.id_user_ml,
    )
    return {"status": "ok", "ho_so": profile}


@app.get("/goi-y/{id_ho_so}")
def goi_y_theo_ho_so(
    id_ho_so: int,
    top_n: int = 10,
    genre: Optional[str] = None,
    authorization: Optional[str] = Header(default=None),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
) -> dict:
    """Lấy gợi ý theo hồ sơ đăng nhập (profile-based route)."""
    user, _ = _require_authenticated_user(authorization=authorization, x_auth_token=x_auth_token)
    store = _require_sql_store()
    profile = store.get_profile(profile_id=id_ho_so, id_nguoi_dung=user.id_nguoi_dung)
    if profile is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy hồ sơ")

    preferred_genre = genre or profile.get("the_loai_uu_tien")
    user_ml_id = int(profile["id_user_ml"])
    payload = _require_service().recommend(
        user_id=user_ml_id,
        top_n=int(top_n),
        preferred_genre=preferred_genre,
    )
    store.record_recommendation_history(
        id_ho_so=id_ho_so,
        loai_goi_y=str(payload.get("route", "unknown")),
        top_n=int(top_n),
        the_loai_uu_tien=preferred_genre,
    )
    payload["ho_so"] = {
        "id_ho_so": int(profile["id_ho_so"]),
        "ten_ho_so": profile["ten_ho_so"],
        "che_do_goi_y": profile["che_do_goi_y"],
        "id_user_ml": user_ml_id,
    }
    return payload


@app.post("/phan-hoi")
def phan_hoi(
    payload: ProfileFeedbackRequest,
    background_tasks: BackgroundTasks,
    authorization: Optional[str] = Header(default=None),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
) -> dict:
    """Gửi phản hồi theo hồ sơ: lưu SQL + append feedback pipeline."""
    if payload.diem_danh_gia < 0.5 or payload.diem_danh_gia > 5.0:
        raise HTTPException(status_code=400, detail="Điểm đánh giá phải trong khoảng 0.5-5.0")

    user, _ = _require_authenticated_user(authorization=authorization, x_auth_token=x_auth_token)
    store = _require_sql_store()
    profile = store.get_profile(profile_id=payload.id_ho_so, id_nguoi_dung=user.id_nguoi_dung)
    if profile is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy hồ sơ")

    store.record_rating(
        id_ho_so=int(profile["id_ho_so"]),
        id_phim=payload.id_phim,
        diem_danh_gia=payload.diem_danh_gia,
        nguon_danh_gia="ui",
    )
    service_result = _require_service().add_rating(
        user_id=int(profile["id_user_ml"]),
        movie_id=payload.id_phim,
        rating=payload.diem_danh_gia,
        trigger_retrain=False,
    )
    if payload.retrain:
        background_tasks.add_task(_run_background_retrain)
    return {
        "status": "ok",
        "ho_so": {
            "id_ho_so": int(profile["id_ho_so"]),
            "ten_ho_so": profile["ten_ho_so"],
            "id_user_ml": int(profile["id_user_ml"]),
        },
        "feedback": {
            "id_phim": payload.id_phim,
            "diem_danh_gia": payload.diem_danh_gia,
            "retrain": payload.retrain,
        },
        "pipeline": service_result,
    }


@app.get("/lich-su-goi-y/{id_ho_so}")
def lich_su_goi_y(
    id_ho_so: int,
    limit: int = 20,
    authorization: Optional[str] = Header(default=None),
    x_auth_token: Optional[str] = Header(default=None, alias="X-Auth-Token"),
) -> dict:
    """Lịch sử các lần lấy gợi ý theo hồ sơ."""
    user, _ = _require_authenticated_user(authorization=authorization, x_auth_token=x_auth_token)
    store = _require_sql_store()
    profile = store.get_profile(profile_id=id_ho_so, id_nguoi_dung=user.id_nguoi_dung)
    if profile is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy hồ sơ")
    history = store.list_recommendation_history(id_ho_so=id_ho_so, limit=max(1, min(limit, 100)))
    return {"status": "ok", "id_ho_so": id_ho_so, "history": history}


@app.get("/thong-tin-mo-hinh")
def thong_tin_mo_hinh() -> dict:
    """Thông tin model/artifact cho phần demo sản phẩm."""
    return _require_service().health()


if WEB_DIST_DIR.exists():
    assets_dir = WEB_DIST_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="web-assets")

    @app.get("/", include_in_schema=False)
    def serve_web_root() -> FileResponse:
        return FileResponse(str(WEB_DIST_DIR / "index.html"))

    @app.get("/{full_path:path}", include_in_schema=False)
    def serve_web_spa(full_path: str) -> FileResponse:
        candidate = WEB_DIST_DIR / full_path
        if candidate.exists() and candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(WEB_DIST_DIR / "index.html"))

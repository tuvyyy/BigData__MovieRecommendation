"""SQL-backed account/profile store for product-style serving flows."""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional


def hash_password(raw_password: str) -> str:
    """Hash password using PBKDF2-HMAC-SHA256."""
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        raw_password.encode("utf-8"),
        salt.encode("utf-8"),
        150_000,
    ).hex()
    return f"{salt}${digest}"


def verify_password(raw_password: str, encoded_password: str) -> bool:
    """Verify password against stored salted hash."""
    try:
        salt, digest = encoded_password.split("$", maxsplit=1)
    except ValueError:
        return False

    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        raw_password.encode("utf-8"),
        salt.encode("utf-8"),
        150_000,
    ).hex()
    return secrets.compare_digest(candidate, digest)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class SessionUser:
    id_nguoi_dung: int
    ten_tai_khoan: str
    email: str
    ho_ten: Optional[str]
    vai_tro: str


class SqlStore:
    """SQL layer for account/profile/feedback persistence (SQLite or SQL Server)."""

    def __init__(self, db_path: Path | None = None, config: Dict[str, Any] | None = None) -> None:
        self.config = config or {}
        self.engine = str(self.config.get("engine", "sqlite")).lower()
        self._lock = Lock()

        if self.engine == "sqlserver":
            self._conn = self._connect_sqlserver()
            self._init_schema_sqlserver()
        else:
            # default to sqlite
            self.engine = "sqlite"
            self.db_path = db_path or Path(self.config.get("sqlite_path", "data/sql/recommendation.db"))
            if not self.db_path.is_absolute():
                self.db_path = Path(self.db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA foreign_keys = ON;")
            self._init_schema_sqlite()

    # ------------------------------------------------------------------ #
    # Common helpers
    # ------------------------------------------------------------------ #
    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _build_sqlserver_conn_str(self) -> str:
        driver = self.config.get("driver", "ODBC Driver 17 for SQL Server")
        server = self.config.get("server", "localhost\\SQLEXPRESS")
        database = self.config.get("database", "Movie_Recommendation")
        trusted = "yes" if self.config.get("trusted_connection", True) else "no"
        encrypt = "yes" if self.config.get("encrypt", True) else "no"
        trust_cert = "yes" if self.config.get("trust_server_certificate", True) else "no"
        timeout = int(self.config.get("timeout", 5))
        parts = [
            f"Driver={{{driver}}}",
            f"Server={server}",
            f"Database={database}",
            f"Trusted_Connection={trusted}",
            f"Encrypt={encrypt}",
            f"TrustServerCertificate={trust_cert}",
            f"Connection Timeout={timeout}",
        ]
        return ";".join(parts) + ";"

    def _connect_sqlserver(self):
        try:
            import pyodbc  # type: ignore
        except ImportError as exc:
            raise RuntimeError("pyodbc is required for SQL Server backend; please install pyodbc") from exc

        conn_str = self._build_sqlserver_conn_str()
        conn = pyodbc.connect(conn_str, autocommit=False)
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding="utf-8")
        conn.setencoding(encoding="utf-8")
        return conn

    # ------------------------------------------------------------------ #
    # Schema init
    # ------------------------------------------------------------------ #
    def _init_schema_sqlite(self) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS Nguoi_Dung (
            id_nguoi_dung INTEGER PRIMARY KEY AUTOINCREMENT,
            ten_tai_khoan TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            mat_khau TEXT NOT NULL,
            ho_ten TEXT NULL,
            vai_tro TEXT NOT NULL DEFAULT 'nguoi_dung',
            trang_thai TEXT NOT NULL DEFAULT 'hoat_dong',
            ngay_tao TEXT NOT NULL DEFAULT (datetime('now')),
            ngay_cap_nhat TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS Ho_So (
            id_ho_so INTEGER PRIMARY KEY AUTOINCREMENT,
            id_nguoi_dung INTEGER NOT NULL,
            ten_ho_so TEXT NOT NULL,
            loai_ho_so TEXT NOT NULL DEFAULT 'ca_nhan',
            che_do_goi_y TEXT NOT NULL DEFAULT 'so_thich_ban_dau',
            id_user_ml INTEGER NOT NULL,
            anh_dai_dien TEXT NULL,
            the_loai_uu_tien TEXT NULL,
            trang_thai TEXT NOT NULL DEFAULT 'hoat_dong',
            ngay_tao TEXT NOT NULL DEFAULT (datetime('now')),
            ngay_cap_nhat TEXT NULL,
            FOREIGN KEY (id_nguoi_dung) REFERENCES Nguoi_Dung(id_nguoi_dung)
        );

        CREATE TABLE IF NOT EXISTS Phim (
            id_phim INTEGER PRIMARY KEY,
            tieu_de TEXT NOT NULL,
            the_loai TEXT NULL,
            nam_phat_hanh INTEGER NULL,
            diem_trung_binh REAL NULL,
            so_luot_danh_gia INTEGER NULL,
            ngay_tao TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS Danh_Gia (
            id_danh_gia INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ho_so INTEGER NOT NULL,
            id_phim INTEGER NOT NULL,
            diem_danh_gia REAL NOT NULL,
            nguon_danh_gia TEXT NOT NULL DEFAULT 'ui',
            thoi_gian_danh_gia TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (id_ho_so) REFERENCES Ho_So(id_ho_so),
            FOREIGN KEY (id_phim) REFERENCES Phim(id_phim),
            UNIQUE (id_ho_so, id_phim)
        );

        CREATE TABLE IF NOT EXISTS Goi_Y_Ca_Nhan (
            id_goi_y INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ho_so INTEGER NOT NULL,
            id_phim INTEGER NOT NULL,
            diem_du_doan REAL NOT NULL,
            thu_hang INTEGER NOT NULL,
            phien_ban_mo_hinh TEXT NULL,
            thoi_gian_tao TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (id_ho_so) REFERENCES Ho_So(id_ho_so),
            FOREIGN KEY (id_phim) REFERENCES Phim(id_phim)
        );

        CREATE TABLE IF NOT EXISTS Goi_Y_Cold_Start (
            id_goi_y INTEGER PRIMARY KEY AUTOINCREMENT,
            the_loai TEXT NOT NULL,
            id_phim INTEGER NOT NULL,
            diem_goi_y REAL NULL,
            so_luot_danh_gia INTEGER NULL,
            thu_hang INTEGER NOT NULL,
            thoi_gian_tao TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (id_phim) REFERENCES Phim(id_phim)
        );

        CREATE TABLE IF NOT EXISTS Lich_Su_Goi_Y (
            id_lich_su INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ho_so INTEGER NOT NULL,
            loai_goi_y TEXT NOT NULL,
            top_n INTEGER NOT NULL,
            the_loai_uu_tien TEXT NULL,
            thoi_gian_tao TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (id_ho_so) REFERENCES Ho_So(id_ho_so)
        );

        CREATE TABLE IF NOT EXISTS Phien_Dang_Nhap (
            id_phien INTEGER PRIMARY KEY AUTOINCREMENT,
            id_nguoi_dung INTEGER NOT NULL,
            token TEXT NOT NULL UNIQUE,
            thoi_gian_dang_nhap TEXT NOT NULL DEFAULT (datetime('now')),
            thoi_gian_het_han TEXT NOT NULL,
            trang_thai TEXT NOT NULL DEFAULT 'hieu_luc',
            FOREIGN KEY (id_nguoi_dung) REFERENCES Nguoi_Dung(id_nguoi_dung)
        );

        CREATE INDEX IF NOT EXISTS idx_ho_so_nguoi_dung ON Ho_So(id_nguoi_dung);
        CREATE INDEX IF NOT EXISTS idx_danh_gia_ho_so ON Danh_Gia(id_ho_so);
        CREATE INDEX IF NOT EXISTS idx_lich_su_goi_y_ho_so ON Lich_Su_Goi_Y(id_ho_so);
        CREATE INDEX IF NOT EXISTS idx_phien_dang_nhap_token ON Phien_Dang_Nhap(token);
        """
        with self._lock:
            self._conn.executescript(schema)
            self._conn.commit()

    def _init_schema_sqlserver(self) -> None:
        ddl_statements = [
            """
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Nguoi_Dung]') AND type = N'U')
            BEGIN
                CREATE TABLE dbo.Nguoi_Dung (
                    id_nguoi_dung INT IDENTITY(1,1) PRIMARY KEY,
                    ten_tai_khoan NVARCHAR(255) NOT NULL UNIQUE,
                    email NVARCHAR(255) NOT NULL UNIQUE,
                    mat_khau NVARCHAR(512) NOT NULL,
                    ho_ten NVARCHAR(255) NULL,
                    vai_tro NVARCHAR(50) NOT NULL DEFAULT 'nguoi_dung',
                    trang_thai NVARCHAR(50) NOT NULL DEFAULT 'hoat_dong',
                    ngay_tao DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    ngay_cap_nhat DATETIME2 NULL
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Ho_So]') AND type = N'U')
            BEGIN
                CREATE TABLE dbo.Ho_So (
                    id_ho_so INT IDENTITY(1,1) PRIMARY KEY,
                    id_nguoi_dung INT NOT NULL,
                    ten_ho_so NVARCHAR(255) NOT NULL,
                    loai_ho_so NVARCHAR(50) NOT NULL DEFAULT 'ca_nhan',
                    che_do_goi_y NVARCHAR(50) NOT NULL DEFAULT 'so_thich_ban_dau',
                    id_user_ml INT NOT NULL,
                    anh_dai_dien NVARCHAR(255) NULL,
                    the_loai_uu_tien NVARCHAR(255) NULL,
                    trang_thai NVARCHAR(50) NOT NULL DEFAULT 'hoat_dong',
                    ngay_tao DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    ngay_cap_nhat DATETIME2 NULL,
                    CONSTRAINT fk_ho_so_user FOREIGN KEY (id_nguoi_dung) REFERENCES dbo.Nguoi_Dung(id_nguoi_dung)
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Phim]') AND type = N'U')
            BEGIN
                CREATE TABLE dbo.Phim (
                    id_phim INT PRIMARY KEY,
                    tieu_de NVARCHAR(500) NOT NULL,
                    the_loai NVARCHAR(255) NULL,
                    nam_phat_hanh INT NULL,
                    diem_trung_binh FLOAT NULL,
                    so_luot_danh_gia INT NULL,
                    ngay_tao DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Danh_Gia]') AND type = N'U')
            BEGIN
                CREATE TABLE dbo.Danh_Gia (
                    id_danh_gia INT IDENTITY(1,1) PRIMARY KEY,
                    id_ho_so INT NOT NULL,
                    id_phim INT NOT NULL,
                    diem_danh_gia FLOAT NOT NULL,
                    nguon_danh_gia NVARCHAR(50) NOT NULL DEFAULT 'ui',
                    thoi_gian_danh_gia DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT fk_danh_gia_ho_so FOREIGN KEY (id_ho_so) REFERENCES dbo.Ho_So(id_ho_so),
                    CONSTRAINT fk_danh_gia_phim FOREIGN KEY (id_phim) REFERENCES dbo.Phim(id_phim),
                    CONSTRAINT uq_danh_gia UNIQUE (id_ho_so, id_phim)
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Goi_Y_Ca_Nhan]') AND type = N'U')
            BEGIN
                CREATE TABLE dbo.Goi_Y_Ca_Nhan (
                    id_goi_y INT IDENTITY(1,1) PRIMARY KEY,
                    id_ho_so INT NOT NULL,
                    id_phim INT NOT NULL,
                    diem_du_doan FLOAT NOT NULL,
                    thu_hang INT NOT NULL,
                    phien_ban_mo_hinh NVARCHAR(100) NULL,
                    thoi_gian_tao DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT fk_goi_y_ca_nhan_ho_so FOREIGN KEY (id_ho_so) REFERENCES dbo.Ho_So(id_ho_so),
                    CONSTRAINT fk_goi_y_ca_nhan_phim FOREIGN KEY (id_phim) REFERENCES dbo.Phim(id_phim)
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Goi_Y_Cold_Start]') AND type = N'U')
            BEGIN
                CREATE TABLE dbo.Goi_Y_Cold_Start (
                    id_goi_y INT IDENTITY(1,1) PRIMARY KEY,
                    the_loai NVARCHAR(255) NOT NULL,
                    id_phim INT NOT NULL,
                    diem_goi_y FLOAT NULL,
                    so_luot_danh_gia INT NULL,
                    thu_hang INT NOT NULL,
                    thoi_gian_tao DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT fk_cold_start_phim FOREIGN KEY (id_phim) REFERENCES dbo.Phim(id_phim)
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Lich_Su_Goi_Y]') AND type = N'U')
            BEGIN
                CREATE TABLE dbo.Lich_Su_Goi_Y (
                    id_lich_su INT IDENTITY(1,1) PRIMARY KEY,
                    id_ho_so INT NOT NULL,
                    loai_goi_y NVARCHAR(50) NOT NULL,
                    top_n INT NOT NULL,
                    the_loai_uu_tien NVARCHAR(255) NULL,
                    thoi_gian_tao DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT fk_lich_su_ho_so FOREIGN KEY (id_ho_so) REFERENCES dbo.Ho_So(id_ho_so)
                );
            END
            """,
            """
            IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Phien_Dang_Nhap]') AND type = N'U')
            BEGIN
                CREATE TABLE dbo.Phien_Dang_Nhap (
                    id_phien INT IDENTITY(1,1) PRIMARY KEY,
                    id_nguoi_dung INT NOT NULL,
                    token NVARCHAR(255) NOT NULL UNIQUE,
                    thoi_gian_dang_nhap DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    thoi_gian_het_han DATETIME2 NOT NULL,
                    trang_thai NVARCHAR(50) NOT NULL DEFAULT 'hieu_luc',
                    CONSTRAINT fk_phien_user FOREIGN KEY (id_nguoi_dung) REFERENCES dbo.Nguoi_Dung(id_nguoi_dung)
                );
            END
            """,
        ]
        with self._lock:
            cursor = self._conn.cursor()
            for ddl in ddl_statements:
                cursor.execute(ddl)
            self._conn.commit()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _row_to_dict(row: Any | None, columns: Optional[List[str]] = None) -> Dict[str, Any] | None:
        if row is None:
            return None
        if hasattr(row, "keys"):
            return {key: row[key] for key in row.keys()}
        if columns is None:
            return None
        return {col: row[idx] for idx, col in enumerate(columns)}

    # ------------------------------------------------------------------ #
    # Seed + core ops
    # ------------------------------------------------------------------ #
    def seed_demo_account(self) -> Dict[str, Any]:
        existing = self.get_user_by_username_or_email("demo")
        if existing:
            return {"seeded": False, "ten_tai_khoan": "demo"}

        user_id = self.create_user(
            ten_tai_khoan="demo",
            email="demo@khangdauti.local",
            mat_khau_raw="demo123",
            ho_ten="Demo User",
        )
        self.create_profile(
            id_nguoi_dung=user_id,
            ten_ho_so="Ho so ca nhan hoa",
            che_do_goi_y="ca_nhan_hoa",
            the_loai_uu_tien="Drama|Action",
            id_user_ml=2,
        )
        self.create_profile(
            id_nguoi_dung=user_id,
            ten_ho_so="Ho so moi",
            che_do_goi_y="so_thich_ban_dau",
            the_loai_uu_tien="Action",
            id_user_ml=999999,
        )
        return {"seeded": True, "ten_tai_khoan": "demo"}

    def get_user_by_username_or_email(self, login_value: str) -> Dict[str, Any] | None:
        if self.engine == "sqlserver":
            query = """
            SELECT TOP (1) * FROM Nguoi_Dung
            WHERE LOWER(ten_tai_khoan) = LOWER(?) OR LOWER(email) = LOWER(?)
            """
        else:
            query = """
            SELECT * FROM Nguoi_Dung
            WHERE lower(ten_tai_khoan) = lower(?) OR lower(email) = lower(?)
            LIMIT 1
            """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, (login_value, login_value))
            row = cursor.fetchone()
            columns = [col[0] for col in cursor.description] if cursor.description else None
        return self._row_to_dict(row, columns)

    def create_user(
        self,
        ten_tai_khoan: str,
        email: str,
        mat_khau_raw: str,
        ho_ten: str | None = None,
    ) -> int:
        encoded_password = hash_password(mat_khau_raw)
        if self.engine == "sqlserver":
            query = """
            INSERT INTO Nguoi_Dung (
                ten_tai_khoan, email, mat_khau, ho_ten, vai_tro, trang_thai, ngay_tao
            ) OUTPUT INSERTED.id_nguoi_dung
            VALUES (?, ?, ?, ?, 'nguoi_dung', 'hoat_dong', SYSUTCDATETIME())
            """
            params = (
                ten_tai_khoan.strip(),
                email.strip().lower(),
                encoded_password,
                ho_ten,
            )
        else:
            query = """
            INSERT INTO Nguoi_Dung (
                ten_tai_khoan, email, mat_khau, ho_ten, vai_tro, trang_thai, ngay_tao
            ) VALUES (?, ?, ?, ?, 'nguoi_dung', 'hoat_dong', ?)
            """
            params = (
                ten_tai_khoan.strip(),
                email.strip().lower(),
                encoded_password,
                ho_ten,
                _utc_now_iso(),
            )
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, params)
            user_id = int(cursor.fetchone()[0]) if self.engine == "sqlserver" else int(cursor.lastrowid)
            self._conn.commit()
        return user_id

    def create_profile(
        self,
        id_nguoi_dung: int,
        ten_ho_so: str,
        che_do_goi_y: str = "so_thich_ban_dau",
        the_loai_uu_tien: str | None = None,
        id_user_ml: int | None = None,
    ) -> Dict[str, Any]:
        normalized_mode = "ca_nhan_hoa" if che_do_goi_y == "ca_nhan_hoa" else "so_thich_ban_dau"
        if id_user_ml is None:
            id_user_ml = 2 if normalized_mode == "ca_nhan_hoa" else 900000 + int(id_nguoi_dung)

        if self.engine == "sqlserver":
            query = """
            INSERT INTO Ho_So (
                id_nguoi_dung,
                ten_ho_so,
                loai_ho_so,
                che_do_goi_y,
                id_user_ml,
                the_loai_uu_tien,
                trang_thai,
                ngay_tao
            ) OUTPUT INSERTED.id_ho_so
            VALUES (?, ?, 'ca_nhan', ?, ?, ?, 'hoat_dong', SYSUTCDATETIME())
            """
            params = (
                id_nguoi_dung,
                ten_ho_so.strip(),
                normalized_mode,
                int(id_user_ml),
                the_loai_uu_tien,
            )
        else:
            query = """
            INSERT INTO Ho_So (
                id_nguoi_dung,
                ten_ho_so,
                loai_ho_so,
                che_do_goi_y,
                id_user_ml,
                the_loai_uu_tien,
                trang_thai,
                ngay_tao
            ) VALUES (?, ?, 'ca_nhan', ?, ?, ?, 'hoat_dong', ?)
            """
            params = (
                id_nguoi_dung,
                ten_ho_so.strip(),
                normalized_mode,
                int(id_user_ml),
                the_loai_uu_tien,
                _utc_now_iso(),
            )
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, params)
            profile_id = int(cursor.fetchone()[0]) if self.engine == "sqlserver" else int(cursor.lastrowid)
            self._conn.commit()
        profile = self.get_profile(profile_id=profile_id, id_nguoi_dung=id_nguoi_dung)
        assert profile is not None
        return profile

    def list_profiles(self, id_nguoi_dung: int) -> List[Dict[str, Any]]:
        query = """
        SELECT id_ho_so, id_nguoi_dung, ten_ho_so, che_do_goi_y, id_user_ml, the_loai_uu_tien, ngay_tao
        FROM Ho_So
        WHERE id_nguoi_dung = ? AND trang_thai = 'hoat_dong'
        ORDER BY id_ho_so ASC
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, (id_nguoi_dung,))
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description] if cursor.description else None
        return [self._row_to_dict(row, columns) for row in rows]

    def get_profile(self, profile_id: int, id_nguoi_dung: int | None = None) -> Dict[str, Any] | None:
        params: tuple[Any, ...]
        if self.engine == "sqlserver":
            query = """
            SELECT TOP (1) id_ho_so, id_nguoi_dung, ten_ho_so, che_do_goi_y, id_user_ml, the_loai_uu_tien, trang_thai, ngay_tao
            FROM Ho_So
            WHERE id_ho_so = ?
            """
            params = (profile_id,)
            if id_nguoi_dung is not None:
                query += " AND id_nguoi_dung = ?"
                params = (profile_id, id_nguoi_dung)
        else:
            query = """
            SELECT id_ho_so, id_nguoi_dung, ten_ho_so, che_do_goi_y, id_user_ml, the_loai_uu_tien, trang_thai, ngay_tao
            FROM Ho_So
            WHERE id_ho_so = ?
            """
            params = (profile_id,)
            if id_nguoi_dung is not None:
                query += " AND id_nguoi_dung = ?"
                params = (profile_id, id_nguoi_dung)
            query += " LIMIT 1"
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, params)
            row = cursor.fetchone()
            columns = [col[0] for col in cursor.description] if cursor.description else None
        return self._row_to_dict(row, columns)

    def create_session(self, id_nguoi_dung: int, ttl_hours: int = 24) -> str:
        token = secrets.token_urlsafe(32)
        issued_at = _utc_now_iso()
        expires_at = (datetime.now(UTC) + timedelta(hours=ttl_hours)).replace(microsecond=0).isoformat()
        if self.engine == "sqlserver":
            query = """
            INSERT INTO Phien_Dang_Nhap (
                id_nguoi_dung, token, thoi_gian_dang_nhap, thoi_gian_het_han, trang_thai
            ) VALUES (?, ?, SYSUTCDATETIME(), ?, 'hieu_luc')
            """
            params = (id_nguoi_dung, token, expires_at)
        else:
            query = """
            INSERT INTO Phien_Dang_Nhap (
                id_nguoi_dung, token, thoi_gian_dang_nhap, thoi_gian_het_han, trang_thai
            ) VALUES (?, ?, ?, ?, 'hieu_luc')
            """
            params = (id_nguoi_dung, token, issued_at, expires_at)
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, params)
            self._conn.commit()
        return token

    def resolve_session_user(self, token: str) -> SessionUser | None:
        if self.engine == "sqlserver":
            query = """
            SELECT TOP (1)
                p.id_phien,
                p.id_nguoi_dung,
                p.thoi_gian_het_han,
                p.trang_thai,
                n.ten_tai_khoan,
                n.email,
                n.ho_ten,
                n.vai_tro
            FROM Phien_Dang_Nhap p
            JOIN Nguoi_Dung n ON n.id_nguoi_dung = p.id_nguoi_dung
            WHERE p.token = ?
            """
        else:
            query = """
            SELECT
                p.id_phien,
                p.id_nguoi_dung,
                p.thoi_gian_het_han,
                p.trang_thai,
                n.ten_tai_khoan,
                n.email,
                n.ho_ten,
                n.vai_tro
            FROM Phien_Dang_Nhap p
            JOIN Nguoi_Dung n ON n.id_nguoi_dung = p.id_nguoi_dung
            WHERE p.token = ?
            LIMIT 1
            """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, (token,))
            row = cursor.fetchone()
            columns = [col[0] for col in cursor.description] if cursor.description else None
        record = self._row_to_dict(row, columns)
        if record is None or record.get("trang_thai") != "hieu_luc":
            return None

        raw_expiry = record.get("thoi_gian_het_han")
        try:
            expires_at = raw_expiry if isinstance(raw_expiry, datetime) else datetime.fromisoformat(str(raw_expiry))
        except ValueError:
            return None
        if datetime.now(UTC) >= expires_at:
            self.invalidate_session(token)
            return None

        return SessionUser(
            id_nguoi_dung=int(record["id_nguoi_dung"]),
            ten_tai_khoan=str(record["ten_tai_khoan"]),
            email=str(record["email"]),
            ho_ten=record.get("ho_ten"),
            vai_tro=str(record.get("vai_tro", "nguoi_dung")),
        )

    def invalidate_session(self, token: str) -> None:
        query = """
        UPDATE Phien_Dang_Nhap
        SET trang_thai = 'het_hieu_luc'
        WHERE token = ?
        """
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, (token,))
            self._conn.commit()

    def record_rating(
        self,
        id_ho_so: int,
        id_phim: int,
        diem_danh_gia: float,
        nguon_danh_gia: str = "ui",
    ) -> Dict[str, Any]:
        rated_at = _utc_now_iso()
        with self._lock:
            cursor = self._conn.cursor()
            if self.engine == "sqlserver":
                cursor.execute(
                    """
                    IF NOT EXISTS (SELECT 1 FROM Phim WHERE id_phim = ?)
                        INSERT INTO Phim (id_phim, tieu_de, the_loai, nam_phat_hanh, diem_trung_binh, so_luot_danh_gia)
                        VALUES (?, 'Unknown', NULL, NULL, NULL, NULL);
                    """,
                    (id_phim, id_phim),
                )
                cursor.execute(
                    """
                    MERGE Danh_Gia AS target
                    USING (SELECT ? AS id_ho_so, ? AS id_phim) AS src
                    ON target.id_ho_so = src.id_ho_so AND target.id_phim = src.id_phim
                    WHEN MATCHED THEN UPDATE SET
                        diem_danh_gia = ?,
                        nguon_danh_gia = ?,
                        thoi_gian_danh_gia = ?
                    WHEN NOT MATCHED THEN INSERT (id_ho_so, id_phim, diem_danh_gia, nguon_danh_gia, thoi_gian_danh_gia)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (
                        id_ho_so,
                        id_phim,
                        float(diem_danh_gia),
                        nguon_danh_gia,
                        rated_at,
                        id_ho_so,
                        id_phim,
                        float(diem_danh_gia),
                        nguon_danh_gia,
                        rated_at,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO Phim (id_phim, tieu_de, ngay_tao)
                    VALUES (?, 'Unknown', datetime('now'))
                    ON CONFLICT(id_phim) DO NOTHING
                    """,
                    (id_phim,),
                )
                cursor.execute(
                    """
                    INSERT INTO Danh_Gia (id_ho_so, id_phim, diem_danh_gia, nguon_danh_gia, thoi_gian_danh_gia)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id_ho_so, id_phim) DO UPDATE SET
                        diem_danh_gia = excluded.diem_danh_gia,
                        nguon_danh_gia = excluded.nguon_danh_gia,
                        thoi_gian_danh_gia = excluded.thoi_gian_danh_gia
                    """,
                    (id_ho_so, id_phim, float(diem_danh_gia), nguon_danh_gia, rated_at),
                )
            self._conn.commit()
        return {
            "id_ho_so": int(id_ho_so),
            "id_phim": int(id_phim),
            "diem_danh_gia": float(diem_danh_gia),
            "thoi_gian_danh_gia": rated_at,
        }

    def record_recommendation_history(
        self,
        id_ho_so: int,
        loai_goi_y: str,
        top_n: int,
        the_loai_uu_tien: str | None = None,
    ) -> None:
        if self.engine == "sqlserver":
            query = """
            INSERT INTO Lich_Su_Goi_Y (id_ho_so, loai_goi_y, top_n, the_loai_uu_tien, thoi_gian_tao)
            VALUES (?, ?, ?, ?, SYSUTCDATETIME())
            """
            params = (id_ho_so, loai_goi_y, int(top_n), the_loai_uu_tien)
        else:
            query = """
            INSERT INTO Lich_Su_Goi_Y (id_ho_so, loai_goi_y, top_n, the_loai_uu_tien, thoi_gian_tao)
            VALUES (?, ?, ?, ?, ?)
            """
            params = (id_ho_so, loai_goi_y, int(top_n), the_loai_uu_tien, _utc_now_iso())
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, params)
            self._conn.commit()

    def list_recommendation_history(self, id_ho_so: int, limit: int = 20) -> List[Dict[str, Any]]:
        if self.engine == "sqlserver":
            query = """
            SELECT TOP (?) id_lich_su, id_ho_so, loai_goi_y, top_n, the_loai_uu_tien, thoi_gian_tao
            FROM Lich_Su_Goi_Y
            WHERE id_ho_so = ?
            ORDER BY id_lich_su DESC
            """
            params = (int(limit), id_ho_so)
        else:
            query = """
            SELECT id_lich_su, id_ho_so, loai_goi_y, top_n, the_loai_uu_tien, thoi_gian_tao
            FROM Lich_Su_Goi_Y
            WHERE id_ho_so = ?
            ORDER BY id_lich_su DESC
            LIMIT ?
            """
            params = (id_ho_so, int(limit))
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description] if cursor.description else None
        return [self._row_to_dict(row, columns) for row in rows]

import React, { useEffect, useMemo, useState } from "react";
import { motion, AnimatePresence } from "motion/react";
import {
  Eye,
  EyeOff,
  Lock,
  Mail,
  User,
  ArrowLeft,
  ShieldCheck,
  RefreshCcw,
  LogOut,
} from "lucide-react";

const API_BASE_URL = (import.meta.env?.VITE_API_BASE_URL || "http://127.0.0.1:8000").replace(/\/$/, "");

function InputField({
  label,
  type = "text",
  placeholder,
  icon: Icon,
  value,
  onChange,
  rightNode,
}) {
  return (
    <label className="block space-y-2">
      <span className="text-sm font-medium text-amber-100/80">{label}</span>
      <div className="flex items-center gap-3 rounded-2xl border border-amber-400/20 bg-white/5 px-4 py-3 shadow-[0_10px_30px_rgba(0,0,0,0.25)] transition focus-within:border-amber-300/60 focus-within:ring-2 focus-within:ring-amber-400/25">
        {Icon ? <Icon className="h-5 w-5 text-amber-200/70" /> : null}
        <input
          type={type}
          value={value}
          onChange={onChange}
          placeholder={placeholder}
          className="w-full bg-transparent text-sm text-slate-100 outline-none placeholder:text-slate-500"
        />
        {rightNode}
      </div>
    </label>
  );
}

function PasswordButton({ show, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="rounded-full p-1 text-amber-200/70 transition hover:bg-amber-200/10 hover:text-amber-100"
    >
      {show ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
    </button>
  );
}

function StatusMessage({ status }) {
  if (!status?.message) return null;
  const styles = {
    error: "border-red-500/40 bg-red-500/10 text-red-100",
    success: "border-emerald-500/40 bg-emerald-500/10 text-emerald-100",
    info: "border-amber-400/30 bg-amber-400/10 text-amber-100",
  };
  const tone = styles[status.type] || styles.info;
  return (
    <div className={`rounded-2xl border px-4 py-3 text-sm ${tone}`}>
      {status.message}
    </div>
  );
}

function AuthLayout({ title, subtitle, children, mode, setMode }) {
  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top,#1b1c1f_0%,#0b0c11_55%,#050506_100%)] p-4 text-slate-100 sm:p-6 lg:p-8">
      <div className="mx-auto grid max-w-6xl gap-6 lg:grid-cols-[1fr_1fr]">
        <div className="relative hidden overflow-hidden rounded-[32px] bg-[linear-gradient(160deg,#0d0f16_0%,#16131f_45%,#0c0a0e_100%)] p-8 text-white lg:flex lg:min-h-[720px] lg:flex-col lg:justify-between">
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(245,197,120,0.22),transparent_40%),radial-gradient(circle_at_bottom_left,rgba(255,255,255,0.08),transparent_30%)]" />

          <div className="relative z-10 flex flex-1 items-center justify-center text-center">
            <div className="space-y-4">
              <h1 className="mx-auto max-w-md text-4xl font-bold leading-tight">{title}</h1>
              <p className="mx-auto max-w-md text-base leading-7 text-white/70">{subtitle}</p>
            </div>
          </div>
        </div>

        <div className="flex min-h-[720px] items-center justify-center rounded-[32px] border border-white/10 bg-white/5 p-6 shadow-[0_30px_80px_rgba(0,0,0,0.45)] backdrop-blur-2xl sm:p-8 lg:p-10">
          <div className="w-full max-w-md">
            <div className="mb-8 flex items-center gap-2 rounded-full border border-white/10 bg-white/5 p-1">
              <button
                onClick={() => setMode("login")}
                className={`flex-1 rounded-full px-4 py-2 text-sm font-medium transition ${
                  mode === "login" ? "bg-amber-400 text-slate-950 shadow" : "text-slate-300 hover:text-white"
                }`}
              >
                Đăng nhập
              </button>
              <button
                onClick={() => setMode("register")}
                className={`flex-1 rounded-full px-4 py-2 text-sm font-medium transition ${
                  mode === "register" ? "bg-amber-400 text-slate-950 shadow" : "text-slate-300 hover:text-white"
                }`}
              >
                Đăng ký
              </button>
              <button
                onClick={() => setMode("forgot")}
                className={`flex-1 rounded-full px-4 py-2 text-sm font-medium transition ${
                  mode === "forgot" ? "bg-amber-400 text-slate-950 shadow" : "text-slate-300 hover:text-white"
                }`}
              >
                Quên Mật khẩu
              </button>
            </div>

            {children}
          </div>
        </div>
      </div>
    </div>
  );
}

function LoginForm({ setMode, onLogin, loading, status, initialLogin }) {
  const [loginId, setLoginId] = useState(initialLogin || "");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);

  useEffect(() => {
    if (initialLogin) {
      setLoginId(initialLogin);
    }
  }, [initialLogin]);

  const handleSubmit = (event) => {
    event.preventDefault();
    onLogin({ loginId, password });
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 18 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -12 }}
      transition={{ duration: 0.25 }}
      className="space-y-6"
    >
      <div className="space-y-2">
        <h2 className="text-3xl font-bold text-white">Đăng nhập</h2>
        <p className="text-sm leading-6 text-slate-300">Nhập email hoặc tên đăng nhập để tiếp tục.</p>
      </div>

      <form className="space-y-4" onSubmit={handleSubmit}>
        <InputField
          label="Email hoặc tên đăng nhập"
          type="text"
          placeholder="demo hoặc example@email.com"
          icon={Mail}
          value={loginId}
          onChange={(e) => setLoginId(e.target.value)}
        />

        <InputField
          label="Mật khẩu"
          type={showPassword ? "text" : "password"}
          placeholder="Nhập mật khẩu"
          icon={Lock}
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          rightNode={<PasswordButton show={showPassword} onClick={() => setShowPassword(!showPassword)} />}
        />

        <div className="flex items-center justify-between text-sm">
          <label className="flex items-center gap-2 text-slate-300">
            <input type="checkbox" className="h-4 w-4 rounded border-white/20 bg-transparent" />
            Ghi nhớ đăng nhập
          </label>
          <button onClick={() => setMode("forgot")} className="font-medium text-amber-200 hover:text-amber-100">
            Quên mật khẩu?
          </button>
        </div>

        <StatusMessage status={status} />

        <button
          type="submit"
          disabled={loading}
          className="w-full rounded-2xl bg-amber-400 px-4 py-3 text-sm font-semibold text-slate-950 shadow-lg transition hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {loading ? "Đang đăng nhập..." : "Đăng nhập"}
        </button>
      </form>

      <p className="text-center text-sm text-slate-300">
        Chưa có tài khoản?{" "}
        <button onClick={() => setMode("register")} className="font-semibold text-amber-200">
          Đăng ký ngay
        </button>
      </p>
    </motion.div>
  );
}

function RegisterForm({ setMode, onRegister, loading, status }) {
  const [fullName, setFullName] = useState("");
  const [username, setUsername] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);

  const handleSubmit = (event) => {
    event.preventDefault();
    onRegister({ fullName, username, email, password, confirmPassword });
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 18 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -12 }}
      transition={{ duration: 0.25 }}
      className="space-y-6"
    >
      <div className="space-y-2">
        <h2 className="text-3xl font-bold text-white">Đăng ký</h2>
        <p className="text-sm leading-6 text-slate-300">Điền đầy đủ thông tin để tạo tài khoản mới.</p>
      </div>

      <form className="space-y-4" onSubmit={handleSubmit}>
        <InputField
          label="Họ và tên"
          placeholder="Nhập họ và tên"
          icon={User}
          value={fullName}
          onChange={(e) => setFullName(e.target.value)}
        />

        <InputField
          label="Tên đăng nhập"
          placeholder="VD: khangdauti"
          icon={User}
          value={username}
          onChange={(e) => setUsername(e.target.value)}
        />

        <InputField
          label="Email"
          type="email"
          placeholder="example@email.com"
          icon={Mail}
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />

        <InputField
          label="Mật khẩu"
          type={showPassword ? "text" : "password"}
          placeholder="Tạo mật khẩu"
          icon={Lock}
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          rightNode={<PasswordButton show={showPassword} onClick={() => setShowPassword(!showPassword)} />}
        />

        <InputField
          label="Xác nhận mật khẩu"
          type={showConfirmPassword ? "text" : "password"}
          placeholder="Nhập lại mật khẩu"
          icon={Lock}
          value={confirmPassword}
          onChange={(e) => setConfirmPassword(e.target.value)}
          rightNode={
            <PasswordButton show={showConfirmPassword} onClick={() => setShowConfirmPassword(!showConfirmPassword)} />
          }
        />

        <StatusMessage status={status} />

        <button
          type="submit"
          disabled={loading}
          className="w-full rounded-2xl bg-amber-400 px-4 py-3 text-sm font-semibold text-slate-950 shadow-lg transition hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {loading ? "Đang tạo tài khoản..." : "Tạo tài khoản"}
        </button>
      </form>

      <p className="text-center text-sm text-slate-300">
        Đã có tài khoản?{" "}
        <button onClick={() => setMode("login")} className="font-semibold text-amber-200">
          Đăng nhập
        </button>
      </p>
    </motion.div>
  );
}

function ForgotPasswordForm({ setMode, onForgot, loading, status }) {
  const [email, setEmail] = useState("");

  const handleSubmit = (event) => {
    event.preventDefault();
    onForgot({ email });
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 18 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -12 }}
      transition={{ duration: 0.25 }}
      className="space-y-6"
    >
      <div className="space-y-2">
        <button
          onClick={() => setMode("login")}
          className="inline-flex items-center gap-2 text-sm font-medium text-slate-300 transition hover:text-white"
        >
          <ArrowLeft className="h-4 w-4" /> Quay lại đăng nhập
        </button>
        <h2 className="text-3xl font-bold text-white">Quên mật khẩu</h2>
        <p className="text-sm leading-6 text-slate-300">
          Nhập email của bạn, hệ thống sẽ gửi yêu cầu đặt lại mật khẩu.
        </p>
      </div>

      <form className="space-y-4" onSubmit={handleSubmit}>
        <InputField
          label="Email"
          type="email"
          placeholder="example@email.com"
          icon={Mail}
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />

        <StatusMessage status={status} />

        <button
          type="submit"
          disabled={loading}
          className="w-full rounded-2xl bg-amber-400 px-4 py-3 text-sm font-semibold text-slate-950 shadow-lg transition hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {loading ? "Đang gửi..." : "Gửi yêu cầu"}
        </button>
      </form>
    </motion.div>
  );
}

export default function AuthSwitcherPage() {
  const [mode, setMode] = useState("login");
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState({ type: "info", message: "" });
  const [profiles, setProfiles] = useState([]);
  const [currentUser, setCurrentUser] = useState(null);
  const [token, setToken] = useState(() => {
    if (typeof window === "undefined") return "";
    return window.localStorage.getItem("mr_token") || "";
  });
  const [selectedProfileId, setSelectedProfileId] = useState(() => {
    if (typeof window === "undefined") return null;
    const raw = window.localStorage.getItem("mr_profile_id");
    return raw ? Number(raw) : null;
  });
  const [loginPrefill, setLoginPrefill] = useState("");

  const maskedToken = useMemo(() => {
    if (!token) return "";
    return `${token.slice(0, 6)}...${token.slice(-4)}`;
  }, [token]);

  const pageData = {
    login: {
      title: "Chào mừng bạn quay trở lại",
      subtitle: "Đăng nhập để tiếp tục truy cập hệ thống, quản lý dữ liệu và sử dụng các chức năng của ứng dụng.",
    },
    register: {
      title: "Tạo tài khoản mới",
      subtitle: "Đăng ký tài khoản để bắt đầu sử dụng hệ thống với trải nghiệm nhanh, trực quan và chuyên nghiệp hơn.",
    },
    forgot: {
      title: "Khôi phục mật khẩu",
      subtitle: "Nhập email đã đăng ký để hệ thống gửi hướng dẫn đặt lại mật khẩu một cách an toàn và nhanh chóng.",
    },
  };

  const setInfo = (message) => setStatus({ type: "info", message });
  const setError = (message) => setStatus({ type: "error", message });
  const setSuccess = (message) => setStatus({ type: "success", message });

  const handleLogin = async ({ loginId, password }) => {
    if (!loginId || !password) {
      setError("Vui lòng nhập đầy đủ tên đăng nhập/email và mật khẩu.");
      return;
    }
    setLoading(true);
    setInfo("Đang xác thực tài khoản...");
    try {
      const response = await fetch(`${API_BASE_URL}/dang-nhap`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tenDangNhap: loginId, matKhau: password }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.detail || "Đăng nhập thất bại.");
      }
      setToken(data.token);
      setCurrentUser(data.nguoi_dung);
      setProfiles(data.ho_so || []);
      if (typeof window !== "undefined") {
        window.localStorage.setItem("mr_token", data.token);
      }
      if (data.ho_so && data.ho_so.length > 0) {
        const firstProfile = data.ho_so[0];
        setSelectedProfileId(firstProfile.id_ho_so);
        if (typeof window !== "undefined") {
          window.localStorage.setItem("mr_profile_id", String(firstProfile.id_ho_so));
        }
      }
      setSuccess(`Đăng nhập thành công. Xin chào ${data?.nguoi_dung?.ten_tai_khoan || "bạn"}!`);
    } catch (error) {
      setError(error.message || "Không thể đăng nhập.");
    } finally {
      setLoading(false);
    }
  };

  const handleRegister = async ({ fullName, username, email, password, confirmPassword }) => {
    if (!username || !email || !password) {
      setError("Vui lòng nhập đầy đủ tên đăng nhập, email và mật khẩu.");
      return;
    }
    if (password !== confirmPassword) {
      setError("Mật khẩu xác nhận không khớp.");
      return;
    }
    setLoading(true);
    setInfo("Đang tạo tài khoản mới...");
    try {
      const response = await fetch(`${API_BASE_URL}/dang-ky`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          tenTaiKhoan: username,
          email,
          matKhau: password,
          hoTen: fullName || null,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.detail || "Đăng ký thất bại.");
      }
      setLoginPrefill(username);
      setMode("login");
      setSuccess("Đăng ký thành công. Vui lòng đăng nhập để tiếp tục.");
    } catch (error) {
      setError(error.message || "Không thể đăng ký.");
    } finally {
      setLoading(false);
    }
  };

  const handleForgot = async ({ email }) => {
    if (!email) {
      setError("Vui lòng nhập email để tiếp tục.");
      return;
    }
    setLoading(true);
    try {
      setInfo("Chức năng đặt lại mật khẩu sẽ được bổ sung ở phiên bản tiếp theo.");
    } finally {
      setLoading(false);
    }
  };

  const handleRefreshProfiles = async () => {
    if (!token) return;
    setLoading(true);
    setInfo("Đang tải danh sách hồ sơ...");
    try {
      const response = await fetch(`${API_BASE_URL}/ho-so`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.detail || "Không thể lấy hồ sơ.");
      }
      setProfiles(data.ho_so || []);
      setSuccess("Đã cập nhật danh sách hồ sơ.");
    } catch (error) {
      setError(error.message || "Không thể lấy hồ sơ.");
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    setToken("");
    setCurrentUser(null);
    setProfiles([]);
    setSelectedProfileId(null);
    if (typeof window !== "undefined") {
      window.localStorage.removeItem("mr_token");
      window.localStorage.removeItem("mr_profile_id");
    }
    setInfo("Đã đăng xuất khỏi phiên hiện tại.");
  };

  return (
    <AuthLayout
      title={pageData[mode].title}
      subtitle={pageData[mode].subtitle}
      mode={mode}
      setMode={setMode}
    >
      <AnimatePresence mode="wait">
        {mode === "login" && (
          <LoginForm
            key="login"
            setMode={setMode}
            onLogin={handleLogin}
            loading={loading}
            status={status}
            initialLogin={loginPrefill}
          />
        )}
        {mode === "register" && (
          <RegisterForm
            key="register"
            setMode={setMode}
            onRegister={handleRegister}
            loading={loading}
            status={status}
          />
        )}
        {mode === "forgot" && (
          <ForgotPasswordForm
            key="forgot"
            setMode={setMode}
            onForgot={handleForgot}
            loading={loading}
            status={status}
          />
        )}
      </AnimatePresence>

      {token ? (
        <div className="mt-8 space-y-4 rounded-2xl border border-white/10 bg-white/5 p-4 text-sm text-slate-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="flex items-center gap-2 text-amber-200">
                <ShieldCheck className="h-4 w-4" />
                Đã đăng nhập
              </p>
              <p className="text-xs text-slate-400">
                {currentUser?.ten_tai_khoan || "Tài khoản"} • Token: {maskedToken}
              </p>
            </div>
            <button
              onClick={handleLogout}
              className="inline-flex items-center gap-2 rounded-full border border-white/10 px-3 py-1 text-xs font-medium text-slate-300 transition hover:border-amber-400/40 hover:text-amber-200"
            >
              <LogOut className="h-3.5 w-3.5" />
              Đăng xuất
            </button>
          </div>

          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">Hồ sơ của bạn</p>
              <button
                onClick={handleRefreshProfiles}
                className="inline-flex items-center gap-2 rounded-full border border-white/10 px-3 py-1 text-xs text-slate-300 transition hover:border-amber-400/40 hover:text-amber-200"
              >
                <RefreshCcw className="h-3.5 w-3.5" />
                Làm mới
              </button>
            </div>

            {profiles.length === 0 ? (
              <p className="text-sm text-slate-400">Chưa có hồ sơ. Hãy tạo thêm hồ sơ trong bước tiếp theo.</p>
            ) : (
              <div className="space-y-2">
                {profiles.map((profile) => (
                  <label
                    key={profile.id_ho_so}
                    className="flex items-center justify-between rounded-xl border border-white/10 bg-black/20 px-3 py-2"
                  >
                    <div>
                      <p className="text-sm font-medium text-white">{profile.ten_ho_so}</p>
                      <p className="text-xs text-slate-400">
                        Chế độ: {profile.che_do_goi_y} • ML user: {profile.id_user_ml}
                      </p>
                    </div>
                    <input
                      type="radio"
                      name="profile"
                      checked={selectedProfileId === profile.id_ho_so}
                      onChange={() => {
                        setSelectedProfileId(profile.id_ho_so);
                        if (typeof window !== "undefined") {
                          window.localStorage.setItem("mr_profile_id", String(profile.id_ho_so));
                        }
                      }}
                      className="h-4 w-4 accent-amber-400"
                    />
                  </label>
                ))}
              </div>
            )}
          </div>
        </div>
      ) : null}
    </AuthLayout>
  );
}

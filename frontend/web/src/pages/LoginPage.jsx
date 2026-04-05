import React, { useState } from "react";
import { motion } from "motion/react";
import { Lock, Mail } from "lucide-react";
import AuthLayout from "../components/AuthLayout";
import InputField from "../components/InputField";
import PasswordButton from "../components/PasswordButton";

export default function LoginPage() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);

  return (
    <AuthLayout
      title="Chào mừng bạn quay trở lại"
      subtitle="Đăng nhập để tiếp tục truy cập hệ thống, quản lý dữ liệu và sử dụng các chức năng của ứng dụng."
    >
      <motion.div
        initial={{ opacity: 0, y: 18 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        className="space-y-6"
      >
        <div className="space-y-2">
          <h2 className="text-3xl font-bold text-slate-900">Đăng nhập</h2>
          <p className="text-sm leading-6 text-slate-500">Nhập email và mật khẩu để đăng nhập.</p>
        </div>

        <div className="space-y-4">
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
            placeholder="Nhập mật khẩu"
            icon={Lock}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            rightNode={<PasswordButton show={showPassword} onClick={() => setShowPassword(!showPassword)} />}
          />
        </div>

        <div className="flex items-center justify-between text-sm">
          <label className="flex items-center gap-2 text-slate-600">
            <input type="checkbox" className="h-4 w-4 rounded border-slate-300" />
            Ghi nhớ đăng nhập
          </label>
          <button className="font-medium text-slate-900 hover:opacity-70">Quên mật khẩu?</button>
        </div>

        <button className="w-full rounded-2xl bg-slate-900 px-4 py-3 text-sm font-semibold text-white shadow-lg transition hover:-translate-y-0.5">
          Đăng nhập
        </button>

        <p className="text-center text-sm text-slate-500">
          Chưa có tài khoản? <span className="font-semibold text-slate-900">Đăng ký ngay</span>
        </p>
      </motion.div>
    </AuthLayout>
  );
}
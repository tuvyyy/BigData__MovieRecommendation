import React, { useState } from "react";
import { motion } from "motion/react";
import { Lock, Mail, User } from "lucide-react";
import AuthLayout from "../components/AuthLayout";
import InputField from "../components/InputField";
import PasswordButton from "../components/PasswordButton";

export default function RegisterPage() {
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);

  return (
    <AuthLayout
      title="Tạo tài khoản mới"
      subtitle="Đăng ký tài khoản để bắt đầu sử dụng hệ thống với trải nghiệm nhanh, trực quan và chuyên nghiệp hơn."
    >
      <motion.div
        initial={{ opacity: 0, y: 18 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        className="space-y-6"
      >
        <div className="space-y-2">
          <h2 className="text-3xl font-bold text-slate-900">Đăng ký</h2>
          <p className="text-sm leading-6 text-slate-500">Điền đầy đủ thông tin để tạo tài khoản mới.</p>
        </div>

        <div className="space-y-4">
          <InputField
            label="Họ và tên"
            placeholder="Nhập họ và tên"
            icon={User}
            value={fullName}
            onChange={(e) => setFullName(e.target.value)}
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
              <PasswordButton
                show={showConfirmPassword}
                onClick={() => setShowConfirmPassword(!showConfirmPassword)}
              />
            }
          />
        </div>

        <button className="w-full rounded-2xl bg-slate-900 px-4 py-3 text-sm font-semibold text-white shadow-lg transition hover:-translate-y-0.5">
          Tạo tài khoản
        </button>

        <p className="text-center text-sm text-slate-500">
          Đã có tài khoản? <span className="font-semibold text-slate-900">Đăng nhập</span>
        </p>
      </motion.div>
    </AuthLayout>
  );
}
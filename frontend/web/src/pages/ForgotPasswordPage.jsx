import React, { useState } from "react";
import { motion } from "motion/react";
import { ArrowLeft, Mail } from "lucide-react";
import AuthLayout from "../components/AuthLayout";
import InputField from "../components/InputField";

export default function ForgotPasswordPage() {
  const [email, setEmail] = useState("");

  return (
    <AuthLayout
      title="Khôi phục mật khẩu"
      subtitle="Nhập email đã đăng ký để hệ thống gửi hướng dẫn đặt lại mật khẩu một cách an toàn và nhanh chóng."
    >
      <motion.div
        initial={{ opacity: 0, y: 18 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
        className="space-y-6"
      >
        <div className="space-y-2">
          <button className="inline-flex items-center gap-2 text-sm font-medium text-slate-500 transition hover:text-slate-800">
            <ArrowLeft className="h-4 w-4" /> Quay lại đăng nhập
          </button>
          <h2 className="text-3xl font-bold text-slate-900">Quên mật khẩu</h2>
          <p className="text-sm leading-6 text-slate-500">
            Nhập email của bạn, hệ thống sẽ gửi yêu cầu đặt lại mật khẩu.
          </p>
        </div>

        <InputField
          label="Email"
          type="email"
          placeholder="example@email.com"
          icon={Mail}
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />

        <button className="w-full rounded-2xl bg-slate-900 px-4 py-3 text-sm font-semibold text-white shadow-lg transition hover:-translate-y-0.5">
          Gửi yêu cầu
        </button>
      </motion.div>
    </AuthLayout>
  );
}
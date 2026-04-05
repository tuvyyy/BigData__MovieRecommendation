import React from "react";
import { ShieldCheck } from "lucide-react";

export default function AuthLayout({ title, subtitle, children }) {
  return (
    <div className="min-h-screen bg-[linear-gradient(135deg,#f8fafc_0%,#e0e7ff_45%,#f8fafc_100%)] p-4 sm:p-6 lg:p-8">
      <div className="mx-auto grid max-w-6xl gap-6 lg:grid-cols-[1fr_1fr]">
        <div className="relative hidden overflow-hidden rounded-[32px] bg-slate-900 p-8 text-white lg:flex lg:min-h-[720px] lg:flex-col lg:justify-between">
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(255,255,255,0.16),transparent_25%),radial-gradient(circle_at_bottom_left,rgba(255,255,255,0.10),transparent_22%)]" />

          <div className="relative z-10 flex items-center gap-3">
            <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-white/10 backdrop-blur">
              <ShieldCheck className="h-6 w-6" />
            </div>
            <div>
              <p className="text-sm uppercase tracking-[0.25em] text-white/60">React UI</p>
              <h2 className="text-xl font-semibold">Authentication Pages</h2>
            </div>
          </div>

          <div className="relative z-10 space-y-4">
            <h1 className="max-w-md text-4xl font-bold leading-tight">{title}</h1>
            <p className="max-w-md text-base leading-7 text-white/75">{subtitle}</p>
          </div>

          <div className="relative z-10 grid gap-4 sm:grid-cols-2">
            <div className="rounded-3xl border border-white/10 bg-white/5 p-5 backdrop-blur-sm">
              <h3 className="mb-2 font-semibold">Thiết kế hiện đại</h3>
              <p className="text-sm leading-6 text-white/70">
                Bố cục rõ ràng, bo góc lớn, phù hợp cho đồ án hoặc website quản lý.
              </p>
            </div>
            <div className="rounded-3xl border border-white/10 bg-white/5 p-5 backdrop-blur-sm">
              <h3 className="mb-2 font-semibold">Dễ tích hợp API</h3>
              <p className="text-sm leading-6 text-white/70">
                Có thể nối backend cho login, register và forgot password sau này.
              </p>
            </div>
          </div>
        </div>

        <div className="flex min-h-[720px] items-center justify-center rounded-[32px] border border-white/70 bg-white/80 p-6 shadow-2xl backdrop-blur-xl sm:p-8 lg:p-10">
          <div className="w-full max-w-md">{children}</div>
        </div>
      </div>
    </div>
  );
}
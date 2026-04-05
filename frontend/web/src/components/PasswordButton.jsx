import React from "react";
import { Eye, EyeOff } from "lucide-react";

export default function PasswordButton({ show, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="rounded-full p-1 text-slate-500 transition hover:bg-slate-100 hover:text-slate-700"
    >
      {show ? <EyeOff className="h-5 w-5" /> : <Eye className="h-5 w-5" />}
    </button>
  );
}
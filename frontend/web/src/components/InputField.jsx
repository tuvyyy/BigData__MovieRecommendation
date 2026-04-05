import React from "react";

export default function InputField({
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
      <span className="text-sm font-medium text-slate-700">{label}</span>
      <div className="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white px-4 py-3 shadow-sm transition focus-within:border-slate-400 focus-within:shadow-md">
        {Icon ? <Icon className="h-5 w-5 text-slate-400" /> : null}
        <input
          type={type}
          value={value}
          onChange={onChange}
          placeholder={placeholder}
          className="w-full bg-transparent text-sm text-slate-800 outline-none placeholder:text-slate-400"
        />
        {rightNode}
      </div>
    </label>
  );
}
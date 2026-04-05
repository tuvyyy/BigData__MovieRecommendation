import { useEffect, useState } from "react";
import AuthSwitcherPage from "./AuthSwitcherPage";
import LegacyApp from "./legacy/LegacyApp";

function getCurrentPath() {
  if (typeof window === "undefined") return "/";
  return window.location.pathname || "/";
}

function getAuthToken() {
  if (typeof window === "undefined") return "";
  return window.localStorage.getItem("mr_token") || window.localStorage.getItem("khangdauti_token") || "";
}

export default function App() {
  const [path, setPath] = useState(getCurrentPath);
  const [token, setToken] = useState(getAuthToken);

  useEffect(() => {
    if (typeof window === "undefined") return undefined;

    const syncRouteState = () => {
      setPath(getCurrentPath());
      setToken(getAuthToken());
    };

    window.addEventListener("popstate", syncRouteState);
    window.addEventListener("storage", syncRouteState);

    return () => {
      window.removeEventListener("popstate", syncRouteState);
      window.removeEventListener("storage", syncRouteState);
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;

    if (path === "/") {
      const nextPath = token ? "/recommend" : "/auth";
      if (window.location.pathname !== nextPath) {
        window.history.replaceState(null, "", nextPath);
      }
      setPath(nextPath);
      return;
    }

    if (path.startsWith("/recommend") && !token) {
      window.history.replaceState(null, "", "/auth");
      setPath("/auth");
    }
  }, [path, token]);

  if (path.startsWith("/recommend")) {
    return token ? <LegacyApp /> : null;
  }

  return <AuthSwitcherPage />;
}

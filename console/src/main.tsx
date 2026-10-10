import "@fontsource-variable/ibm-plex-sans";
import "@fontsource/ibm-plex-mono/400.css";
import "@fontsource/ibm-plex-mono/500.css";
import "@fontsource/ibm-plex-mono/600.css";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import App from "./App";
import { StoreProvider } from "./lib/store";
import "./index.css";

const storedTheme = localStorage.getItem("groundplane-theme");
document.documentElement.classList.toggle("dark", storedTheme !== "light");

const router = createBrowserRouter([
  {
    path: "*",
    element: (
      <StoreProvider>
        <App />
      </StoreProvider>
    ),
  },
]);
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
);

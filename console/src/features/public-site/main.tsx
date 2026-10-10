import "@fontsource-variable/ibm-plex-sans";
import "@fontsource/ibm-plex-mono/latin-400.css";
import "./site.css";
import { createRoot, hydrateRoot } from "react-dom/client";
import { LandingPage } from "./page";

const root = document.getElementById("root")!;
if (root.hasChildNodes()) hydrateRoot(root, <LandingPage />);
else createRoot(root).render(<LandingPage />);

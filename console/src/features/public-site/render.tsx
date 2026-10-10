import { renderToString } from "react-dom/server";
import { LandingPage } from "./page";

export const render = () => renderToString(<LandingPage />);

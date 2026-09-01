import '@fontsource-variable/geist'
import '@fontsource-variable/geist-mono'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import { StoreProvider } from './lib/store'
import './index.css'

const storedTheme = localStorage.getItem('groundplane-theme')
document.documentElement.classList.toggle('dark', storedTheme !== 'light')
document.documentElement.style.setProperty('--font-geist-sans', '"Geist Variable"')
document.documentElement.style.setProperty('--font-geist-mono', '"Geist Mono Variable"')

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrowserRouter>
      <StoreProvider>
        <App />
      </StoreProvider>
    </BrowserRouter>
  </StrictMode>,
)

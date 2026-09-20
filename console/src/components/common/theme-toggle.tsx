import { useSyncExternalStore } from 'react'
import { Moon, Sun } from 'lucide-react'
import { Button } from '@/components/ui/button'

function subscribe(onChange: () => void) {
  const observer = new MutationObserver(onChange)
  observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
  return () => observer.disconnect()
}

export function ThemeToggle() {
  const dark = useSyncExternalStore(
    subscribe,
    () => document.documentElement.classList.contains('dark'),
    () => true,
  )
  function toggle() {
    document.documentElement.classList.toggle('dark', !dark)
    try {
      localStorage.setItem('groundplane-theme', dark ? 'light' : 'dark')
    } catch {
      /* Theme still applies when storage is unavailable. */
    }
  }
  return (
    <Button variant="outline" onClick={toggle} aria-label={`Switch to ${dark ? 'light' : 'dark'} mode`}>
      {dark ? <Sun /> : <Moon />}
      <span className="hidden sm:inline">{dark ? 'Light' : 'Dark'} mode</span>
    </Button>
  )
}

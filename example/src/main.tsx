import { createRoot } from 'react-dom/client'
import { useState, useEffect } from 'react'
import App from './App.tsx'
import IvmDemo from './IvmDemo.tsx'
import './index.css'

function Router() {
  const [route, setRoute] = useState(location.hash || '#/')

  useEffect(() => {
    const onHash = () => setRoute(location.hash || '#/')
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  return (
    <>
      <nav style={{
        display: 'flex', gap: 16, padding: '12px 24px',
        background: '#1a1a2e', borderBottom: '1px solid #333'
      }}>
        <a href="#/" style={{ color: route === '#/' || route === '' ? '#4fc3f7' : '#888', textDecoration: 'none' }}>
          Live Query Demo
        </a>
        <a href="#/ivm" style={{ color: route === '#/ivm' ? '#4fc3f7' : '#888', textDecoration: 'none' }}>
          IVM vs Re-query
        </a>
      </nav>
      {route === '#/ivm' ? <IvmDemo /> : <App />}
    </>
  )
}

createRoot(document.getElementById('root')!).render(<Router />)

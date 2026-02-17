import { createRoot } from 'react-dom/client'
import { useState, useEffect } from 'react'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Layers, Activity, GitCompare, Home } from 'lucide-react'
import Landing from './Landing'
import App from './App'
import IvmDemo from './IvmDemo'
import './index.css'

function Router() {
  const [route, setRoute] = useState(location.hash || '#/')

  useEffect(() => {
    const onHash = () => setRoute(location.hash || '#/')
    window.addEventListener('hashchange', onHash)
    return () => window.removeEventListener('hashchange', onHash)
  }, [])

  const navigate = (hash: string) => {
    location.hash = hash
    setRoute(hash)
  }

  const currentTab = route === '#/live' ? 'live' : route === '#/ivm' ? 'ivm' : 'home'

  return (
    <div className="min-h-screen bg-background">
      {/* Navigation */}
      <nav className="sticky top-0 z-50 border-b border-border/50 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
        <div className="container mx-auto px-4">
          <div className="flex h-14 items-center justify-between">
            <a
              href="#/"
              className="flex items-center gap-2 font-semibold text-foreground hover:text-primary transition-colors"
              onClick={(e) => { e.preventDefault(); navigate('#/') }}
            >
              <Layers className="w-5 h-5 text-primary" />
              <span className="text-white font-bold tracking-wider">
                CYNOS
              </span>
            </a>

            <Tabs value={currentTab} className="hidden sm:block">
              <TabsList className="bg-muted/50">
                <TabsTrigger
                  value="home"
                  onClick={() => navigate('#/')}
                  className="gap-1.5"
                >
                  <Home className="w-4 h-4" />
                  Home
                </TabsTrigger>
                <TabsTrigger
                  value="live"
                  onClick={() => navigate('#/live')}
                  className="gap-1.5"
                >
                  <Activity className="w-4 h-4" />
                  Live Query
                </TabsTrigger>
                <TabsTrigger
                  value="ivm"
                  onClick={() => navigate('#/ivm')}
                  className="gap-1.5"
                >
                  <GitCompare className="w-4 h-4" />
                  IVM Demo
                </TabsTrigger>
              </TabsList>
            </Tabs>

            {/* Mobile nav */}
            <div className="flex sm:hidden gap-2">
              <button
                onClick={() => navigate('#/')}
                className={`p-2 rounded-md transition-colors ${currentTab === 'home' ? 'bg-muted text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
              >
                <Home className="w-5 h-5" />
              </button>
              <button
                onClick={() => navigate('#/live')}
                className={`p-2 rounded-md transition-colors ${currentTab === 'live' ? 'bg-muted text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
              >
                <Activity className="w-5 h-5" />
              </button>
              <button
                onClick={() => navigate('#/ivm')}
                className={`p-2 rounded-md transition-colors ${currentTab === 'ivm' ? 'bg-muted text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
              >
                <GitCompare className="w-5 h-5" />
              </button>
            </div>
          </div>
        </div>
      </nav>

      {/* Content */}
      <main>
        {route === '#/live' ? (
          <App />
        ) : route === '#/ivm' ? (
          <IvmDemo />
        ) : (
          <Landing onNavigate={navigate} />
        )}
      </main>
    </div>
  )
}

createRoot(document.getElementById('root')!).render(<Router />)

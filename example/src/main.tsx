import { createRoot } from 'react-dom/client'
import { useState, useEffect, lazy, Suspense } from 'react'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Layers, Activity, GitCompare, Home, Code, Loader2 } from 'lucide-react'
import Landing from './Landing'
import App from './App'
import IvmDemo from './IvmDemo'
import QueryBuilder from './QueryBuilder'
import './index.css'

// Lazy load hidden comparison page
const LovefieldComparison = lazy(() => import('./LovefieldComparison'))

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

  const currentTab = route === '#/live' ? 'live' : route === '#/ivm' ? 'ivm' : route === '#/query' ? 'query' : 'home'

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
                  Incremental Query
                </TabsTrigger>
                <TabsTrigger
                  value="query"
                  onClick={() => navigate('#/query')}
                  className="gap-1.5"
                >
                  <Code className="w-4 h-4" />
                  Query Builder
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
              <button
                onClick={() => navigate('#/query')}
                className={`p-2 rounded-md transition-colors ${currentTab === 'query' ? 'bg-muted text-foreground' : 'text-muted-foreground hover:text-foreground'}`}
              >
                <Code className="w-5 h-5" />
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
        ) : route === '#/query' ? (
          <QueryBuilder />
        ) : route === '#/lf' ? (
          <Suspense fallback={
            <div className="flex flex-col items-center justify-center min-h-[60vh] gap-4">
              <Loader2 className="w-8 h-8 animate-spin" />
              <p className="text-white/40 text-sm tracking-wider uppercase">Loading...</p>
            </div>
          }>
            <LovefieldComparison />
          </Suspense>
        ) : (
          <Landing onNavigate={navigate} />
        )}
      </main>
    </div>
  )
}

createRoot(document.getElementById('root')!).render(<Router />)

import { useState, useEffect } from 'react'
import { Database, Zap, GitCompare, ArrowRight, Activity, Gauge, Terminal, Box, Cpu } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { TextMorph } from 'torph/react'
import { WaveBackground } from './components/WaveBackground'

interface LandingProps {
  onNavigate: (route: string) => void
}

const MORPH_TEXTS = ['CYNOS', 'REACTIVE', 'INCREMENTAL', 'REAL-TIME', 'CYNOS']

export default function Landing({ onNavigate }: LandingProps) {
  const [textIndex, setTextIndex] = useState(0)

  useEffect(() => {
    const interval = setInterval(() => {
      setTextIndex((prev) => (prev + 1) % MORPH_TEXTS.length)
    }, 3000)
    return () => clearInterval(interval)
  }, [])
  return (
    <div className="min-h-screen grid-pattern">
      {/* Hero Section */}
      <section className="relative overflow-hidden py-24 lg:py-40">
        {/* Particle background */}
        <WaveBackground />

        <div className="container mx-auto px-4 relative">
          <div className="max-w-4xl mx-auto text-center">
            {/* Terminal-style header */}
            <div className="inline-flex items-center gap-2 mb-8 px-4 py-2 border border-white/20 bg-black/50 text-xs tracking-widest uppercase">
              <span className="w-2 h-2 bg-white animate-pulse" />
              <span className="text-white/60">SYS.INIT</span>
              <span className="text-white/40">//</span>
              <span>INCREMENTAL VIEW MAINTENANCE</span>
            </div>

            <h1 className="text-5xl md:text-7xl lg:text-8xl font-bold tracking-tighter mb-6">
              <TextMorph
                as="span"
                className="text-white glow-text inline-block"
                duration={800}
                ease="cubic-bezier(0.16, 1, 0.3, 1)"
              >
                {MORPH_TEXTS[textIndex]}
              </TextMorph>
            </h1>

            <p className="text-lg md:text-xl text-white/50 max-w-2xl mx-auto mb-4 font-light tracking-wide">
              REACTIVE DATABASE ENGINE
            </p>

            <p className="text-sm text-white/60 max-w-xl mx-auto mb-12 font-mono">
              O(δ) query propagation · WebAssembly powered · Real-time incremental updates
            </p>

            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Button
                size="lg"
                onClick={() => onNavigate('#/live')}
                className="gap-3 bg-white text-black hover:bg-white/90 border-0 uppercase tracking-wider text-sm font-medium px-8"
              >
                <Terminal className="w-4 h-4" />
                LIVE QUERY
                <ArrowRight className="w-4 h-4" />
              </Button>
              <Button
                size="lg"
                variant="outline"
                onClick={() => onNavigate('#/ivm')}
                className="gap-3 border-white/30 hover:bg-white/10 hover:border-white/50 uppercase tracking-wider text-sm font-medium px-8"
              >
                <GitCompare className="w-4 h-4" />
                BENCHMARK
              </Button>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="py-24 border-t border-white/10">
        <div className="container mx-auto px-4">
          <div className="text-center mb-16">
            <p className="text-xs tracking-[0.3em] text-white/40 uppercase mb-4">// CAPABILITIES</p>
            <h2 className="text-2xl md:text-3xl font-bold tracking-tight">CORE FEATURES</h2>
          </div>

          <div className="grid md:grid-cols-3 gap-px max-w-5xl mx-auto bg-white/10">
            <div className="bg-background p-8 hover:bg-white/[0.02] transition-colors group">
              <div className="w-10 h-10 border border-white/20 flex items-center justify-center mb-6 group-hover:border-white/40 transition-colors">
                <Zap className="w-5 h-5" />
              </div>
              <h3 className="text-sm font-bold tracking-wider uppercase mb-3">Incremental Updates</h3>
              <p className="text-sm text-white/40 leading-relaxed">
                Only propagate changes, not entire result sets. O(delta) complexity for blazing fast updates.
              </p>
            </div>

            <div className="bg-background p-8 hover:bg-white/[0.02] transition-colors group">
              <div className="w-10 h-10 border border-white/20 flex items-center justify-center mb-6 group-hover:border-white/40 transition-colors">
                <Database className="w-5 h-5" />
              </div>
              <h3 className="text-sm font-bold tracking-wider uppercase mb-3">In-Browser Database</h3>
              <p className="text-sm text-white/40 leading-relaxed">
                Full SQL-like query capabilities running entirely in WebAssembly. No server round-trips.
              </p>
            </div>

            <div className="bg-background p-8 hover:bg-white/[0.02] transition-colors group">
              <div className="w-10 h-10 border border-white/20 flex items-center justify-center mb-6 group-hover:border-white/40 transition-colors">
                <Gauge className="w-5 h-5" />
              </div>
              <h3 className="text-sm font-bold tracking-wider uppercase mb-3">Binary Protocol</h3>
              <p className="text-sm text-white/40 leading-relaxed">
                Efficient binary serialization for query results. Up to 10x faster than JSON parsing.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* Demo Cards Section */}
      <section className="py-24 border-t border-white/10">
        <div className="container mx-auto px-4">
          <div className="text-center mb-16">
            <p className="text-xs tracking-[0.3em] text-white/40 uppercase mb-4">// DEMONSTRATIONS</p>
            <h2 className="text-2xl md:text-3xl font-bold tracking-tight">INTERACTIVE DEMOS</h2>
          </div>

          <div className="grid md:grid-cols-2 gap-6 max-w-4xl mx-auto">
            <div
              className="border border-white/10 bg-white/[0.02] p-8 hover:border-white/30 transition-all cursor-pointer group"
              onClick={() => onNavigate('#/live')}
            >
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                  <span className="w-2 h-2 bg-white animate-pulse" />
                  <span className="text-xs tracking-widest uppercase">LIVE</span>
                </div>
                <ArrowRight className="w-4 h-4 text-white/30 group-hover:text-white transition-colors" />
              </div>
              <h3 className="text-lg font-bold tracking-wide uppercase mb-3">Live Query Demo</h3>
              <p className="text-sm text-white/40 mb-6">
                Watch real-time stock data updates with live query subscriptions.
                Test continuous updates and see cell-level change highlighting.
              </p>
              <div className="flex gap-2 flex-wrap">
                <span className="text-xs px-2 py-1 border border-white/20 text-white/60">observe()</span>
                <span className="text-xs px-2 py-1 border border-white/20 text-white/60">Real-time</span>
                <span className="text-xs px-2 py-1 border border-white/20 text-white/60">100K+ Rows</span>
              </div>
            </div>

            <div
              className="border border-white/10 bg-white/[0.02] p-8 hover:border-white/30 transition-all cursor-pointer group"
              onClick={() => onNavigate('#/ivm')}
            >
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                  <Box className="w-4 h-4" />
                  <span className="text-xs tracking-widest uppercase">BENCHMARK</span>
                </div>
                <ArrowRight className="w-4 h-4 text-white/30 group-hover:text-white transition-colors" />
              </div>
              <h3 className="text-lg font-bold tracking-wide uppercase mb-3">IVM vs Re-query</h3>
              <p className="text-sm text-white/40 mb-6">
                Compare incremental view maintenance (trace) against traditional re-query (observe).
                See the performance difference in real-time.
              </p>
              <div className="flex gap-2 flex-wrap">
                <span className="text-xs px-2 py-1 border border-white/20 text-white/60">trace()</span>
                <span className="text-xs px-2 py-1 border border-white/20 text-white/60">O(delta)</span>
                <span className="text-xs px-2 py-1 border border-white/20 text-white/60">Benchmark</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Stats Section */}
      <section className="py-24 border-t border-white/10">
        <div className="container mx-auto px-4">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-8 max-w-4xl mx-auto">
            <div className="text-center">
              <div className="text-4xl md:text-5xl font-bold mb-2 font-mono">SQL</div>
              <div className="text-xs text-white/40 tracking-widest uppercase">Like Syntax</div>
            </div>
            <div className="text-center">
              <div className="text-4xl md:text-5xl font-bold mb-2 font-mono">1M+</div>
              <div className="text-xs text-white/40 tracking-widest uppercase">Rows Supported</div>
            </div>
            <div className="text-center">
              <div className="text-4xl md:text-5xl font-bold mb-2 font-mono">O(δ)</div>
              <div className="text-xs text-white/40 tracking-widest uppercase">Complexity</div>
            </div>
            <div className="text-center">
              <div className="text-4xl md:text-5xl font-bold mb-2 font-mono">WASM</div>
              <div className="text-xs text-white/40 tracking-widest uppercase">Powered</div>
            </div>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="py-12 border-t border-white/10">
        <div className="container mx-auto px-4 text-center">
          <div className="flex items-center justify-center gap-3 mb-4">
            <div className="w-6 h-px bg-white/20" />
            <span className="text-sm font-bold tracking-widest uppercase">CYNOS</span>
            <div className="w-6 h-px bg-white/20" />
          </div>
          <p className="text-xs text-white/30 tracking-wider">
            HIGH-PERFORMANCE REACTIVE DATABASE FOR THE MODERN WEB
          </p>
        </div>
      </footer>
    </div>
  )
}

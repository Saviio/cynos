import { useEffect, useRef } from 'react'
import * as THREE from 'three'
import { SimplexNoise } from 'three/addons/math/SimplexNoise.js'

export function WaveBackground() {
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!containerRef.current) return

    const container = containerRef.current
    const width = container.clientWidth
    const height = container.clientHeight

    // Scene setup
    const scene = new THREE.Scene()
    scene.background = null

    const camera = new THREE.PerspectiveCamera(30, width / height, 0.1, 100)
    camera.position.set(0, 3, 6)
    camera.lookAt(0, 0, 0)

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true })
    renderer.setSize(width, height)
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))
    container.appendChild(renderer.domElement)

    // Wave geometry
    const geometry = new THREE.PlaneGeometry(8, 6, 120, 80)
    const pos = geometry.getAttribute('position')
    const simplex = new SimplexNoise()

    const waves = new THREE.Points(
      geometry,
      new THREE.PointsMaterial({
        size: 0.015,
        color: 0xffffff,
        transparent: true,
        opacity: 0.1,
        sizeAttenuation: true
      })
    )
    waves.rotation.x = -Math.PI / 2
    scene.add(waves)

    // Animation
    let animationId: number
    const animate = (t: number) => {
      for (let i = 0; i < pos.count; i++) {
        const x = pos.getX(i)
        const y = pos.getY(i)
        const z = 0.4 * simplex.noise3d(x / 2, y / 2, t / 3000)
        pos.setZ(i, z)
      }
      pos.needsUpdate = true
      renderer.render(scene, camera)
      animationId = requestAnimationFrame(animate)
    }
    animationId = requestAnimationFrame(animate)

    // Resize handler
    const handleResize = () => {
      const w = container.clientWidth
      const h = container.clientHeight
      camera.aspect = w / h
      camera.updateProjectionMatrix()
      renderer.setSize(w, h)
    }
    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      cancelAnimationFrame(animationId)
      renderer.dispose()
      geometry.dispose()
      container.removeChild(renderer.domElement)
    }
  }, [])

  return <div ref={containerRef} className="absolute inset-0 w-full h-full" />
}

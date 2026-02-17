/** @type {import('tailwindcss').Config} */
export default {
  darkMode: ["class"],
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        border: "hsl(var(--border))",
        input: "hsl(var(--input))",
        ring: "hsl(var(--ring))",
        background: "hsl(var(--background))",
        foreground: "hsl(var(--foreground))",
        primary: {
          DEFAULT: "hsl(var(--primary))",
          foreground: "hsl(var(--primary-foreground))",
        },
        secondary: {
          DEFAULT: "hsl(var(--secondary))",
          foreground: "hsl(var(--secondary-foreground))",
        },
        destructive: {
          DEFAULT: "hsl(var(--destructive))",
          foreground: "hsl(var(--destructive-foreground))",
        },
        muted: {
          DEFAULT: "hsl(var(--muted))",
          foreground: "hsl(var(--muted-foreground))",
        },
        accent: {
          DEFAULT: "hsl(var(--accent))",
          foreground: "hsl(var(--accent-foreground))",
        },
        card: {
          DEFAULT: "hsl(var(--card))",
          foreground: "hsl(var(--card-foreground))",
        },
      },
      borderRadius: {
        lg: "var(--radius)",
        md: "calc(var(--radius) - 2px)",
        sm: "calc(var(--radius) - 4px)",
      },
      keyframes: {
        "pulse-glow": {
          "0%, 100%": { boxShadow: "0 0 0 0 rgba(255, 255, 255, 0.3)" },
          "50%": { boxShadow: "0 0 20px 4px rgba(255, 255, 255, 0)" },
        },
        "cell-flash": {
          "0%": { backgroundColor: "rgba(255, 255, 255, 0.3)" },
          "100%": { backgroundColor: "transparent" },
        },
        "scan": {
          "0%": { transform: "translateY(-100%)" },
          "100%": { transform: "translateY(100%)" },
        },
        "flicker": {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.8" },
        },
      },
      animation: {
        "pulse-glow": "pulse-glow 2s ease-in-out infinite",
        "cell-flash": "cell-flash 0.3s ease-out",
        "scan": "scan 8s linear infinite",
        "flicker": "flicker 0.1s ease-in-out infinite",
      },
      fontFamily: {
        mono: ['SF Mono', 'Monaco', 'Inconsolata', 'Fira Code', 'monospace'],
      },
    },
  },
  plugins: [],
}

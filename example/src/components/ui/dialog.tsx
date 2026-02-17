import * as React from "react"
import { cn } from "@/lib/utils"
import { X } from "lucide-react"

interface DialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  children: React.ReactNode
}

export function Dialog({ open, onOpenChange, children }: DialogProps) {
  if (!open) return null

  return (
    <div className="fixed inset-0 z-50">
      <div 
        className="fixed inset-0 bg-black/80" 
        onClick={() => onOpenChange(false)}
      />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        {children}
      </div>
    </div>
  )
}

export function DialogContent({ 
  children, 
  className,
  onClose
}: { 
  children: React.ReactNode
  className?: string
  onClose?: () => void
}) {
  return (
    <div className={cn(
      "relative bg-background border border-white/10 shadow-lg max-h-[85vh] overflow-auto",
      className
    )}>
      {onClose && (
        <button
          onClick={onClose}
          className="absolute right-4 top-4 text-white/40 hover:text-white transition-colors"
        >
          <X className="w-4 h-4" />
        </button>
      )}
      {children}
    </div>
  )
}

export function DialogHeader({ children, className }: { children: React.ReactNode, className?: string }) {
  return <div className={cn("border-b border-white/10 px-6 py-4", className)}>{children}</div>
}

export function DialogTitle({ children, className }: { children: React.ReactNode, className?: string }) {
  return <h2 className={cn("text-lg font-semibold tracking-tight uppercase", className)}>{children}</h2>
}

export function DialogDescription({ children, className }: { children: React.ReactNode, className?: string }) {
  return <p className={cn("text-sm text-white/40 mt-1", className)}>{children}</p>
}

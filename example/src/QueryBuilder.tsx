import { useState, useCallback } from 'react'
import { type Stock, STOCK_COLUMNS } from './db'
import { useQueryWorker, type WhereClause } from './useQueryWorker'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Loader2, Play, Plus, Trash2, Database, Code, Clock, Cpu } from 'lucide-react'
import { cn } from '@/lib/utils'

type Operator = 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte'
type SortDir = 'Asc' | 'Desc'
type Operation = 'select' | 'insert' | 'update' | 'delete'

interface UIWhereClause {
  id: string
  field: keyof Stock
  operator: Operator
  value: string
}

interface SortClause {
  field: keyof Stock
  dir: SortDir
}

const OPERATORS: { value: Operator; label: string }[] = [
  { value: 'eq', label: '=' },
  { value: 'neq', label: '≠' },
  { value: 'gt', label: '>' },
  { value: 'gte', label: '≥' },
  { value: 'lt', label: '<' },
  { value: 'lte', label: '≤' },
]

const NUMERIC_FIELDS: (keyof Stock)[] = ['id', 'price', 'change', 'changePercent', 'volume', 'high', 'low', 'open', 'marketCap', 'pe']
const INDEXED_FIELDS: (keyof Stock)[] = ['id', 'price', 'symbol', 'sector']

const getFieldType = (field: keyof Stock): 'number' | 'string' => {
  return NUMERIC_FIELDS.includes(field) ? 'number' : 'string'
}

const isIndexedField = (field: keyof Stock): boolean => {
  return INDEXED_FIELDS.includes(field)
}

export default function QueryBuilder() {
  const {
    ready,
    stockCount,
    executing,
    results,
    execTime,
    latencyTime,
    decodeTime,
    affectedRows,
    error,
    querySelect,
    queryInsert,
    queryUpdate,
    queryDelete,
    clearResults,
  } = useQueryWorker()

  // Query state
  const [operation, setOperation] = useState<Operation>('select')
  const [selectedFields, setSelectedFields] = useState<(keyof Stock)[]>(['id', 'symbol', 'name', 'price', 'sector'])
  const [whereClauses, setWhereClauses] = useState<UIWhereClause[]>([])
  const [sortClause, setSortClause] = useState<SortClause | null>(null)
  const [limit, setLimit] = useState('100')

  // For INSERT/UPDATE
  const [updateField, setUpdateField] = useState<keyof Stock>('price')
  const [updateValue, setUpdateValue] = useState('')
  const [insertCount, setInsertCount] = useState('10')

  const addWhereClause = () => {
    setWhereClauses([...whereClauses, {
      id: crypto.randomUUID(),
      field: 'price',
      operator: 'gt',
      value: '100'
    }])
  }

  const removeWhereClause = (id: string) => {
    setWhereClauses(whereClauses.filter(c => c.id !== id))
  }

  const updateWhereClause = (id: string, updates: Partial<UIWhereClause>) => {
    setWhereClauses(whereClauses.map(c => c.id === id ? { ...c, ...updates } : c))
  }

  const toggleField = (field: keyof Stock) => {
    if (selectedFields.includes(field)) {
      if (selectedFields.length > 1) {
        setSelectedFields(selectedFields.filter(f => f !== field))
        clearResults()
      }
    } else {
      setSelectedFields([...selectedFields, field])
      clearResults()
    }
  }

  // Convert UI where clauses to worker format
  const toWorkerWhere = (clauses: UIWhereClause[]): WhereClause[] => {
    return clauses.map(c => ({
      field: c.field,
      operator: c.operator,
      value: getFieldType(c.field) === 'number' ? Number(c.value) : c.value
    }))
  }

  const generateCode = useCallback((): string => {
    const lines: string[] = []

    if (operation === 'select') {
      const fields = selectedFields.length === STOCK_COLUMNS.length ? "'*'" : selectedFields.map(f => `'${f}'`).join(', ')
      lines.push(`db.select(${fields})`)
      lines.push(`  .from('stocks')`)

      for (const clause of whereClauses) {
        const val = getFieldType(clause.field) === 'number' ? clause.value : `'${clause.value}'`
        if (clause.operator === 'neq') {
          lines.push(`  .where(col('${clause.field}').eq(${val}).not())`)
        } else {
          lines.push(`  .where(col('${clause.field}').${clause.operator}(${val}))`)
        }
      }

      if (sortClause) {
        lines.push(`  .orderBy('${sortClause.field}', '${sortClause.dir}')`)
      }

      if (limit) {
        lines.push(`  .limit(${limit})`)
      }

      lines.push(`  .exec()`)
    } else if (operation === 'insert') {
      lines.push(`// Insert ${insertCount} random stocks`)
      lines.push(`db.insert('stocks')`)
      lines.push(`  .values([...generatedStocks])`)
      lines.push(`  .exec()`)
    } else if (operation === 'update') {
      const val = getFieldType(updateField) === 'number' ? updateValue : `'${updateValue}'`
      lines.push(`db.update('stocks')`)
      lines.push(`  .set('${updateField}', ${val})`)

      for (const clause of whereClauses) {
        const clauseVal = getFieldType(clause.field) === 'number' ? clause.value : `'${clause.value}'`
        if (clause.operator === 'neq') {
          lines.push(`  .where(col('${clause.field}').eq(${clauseVal}).not())`)
        } else {
          lines.push(`  .where(col('${clause.field}').${clause.operator}(${clauseVal}))`)
        }
      }

      lines.push(`  .exec()`)
    } else if (operation === 'delete') {
      lines.push(`db.delete('stocks')`)

      for (const clause of whereClauses) {
        const val = getFieldType(clause.field) === 'number' ? clause.value : `'${clause.value}'`
        if (clause.operator === 'neq') {
          lines.push(`  .where(col('${clause.field}').eq(${val}).not())`)
        } else {
          lines.push(`  .where(col('${clause.field}').${clause.operator}(${val}))`)
        }
      }

      lines.push(`  .exec()`)
    }

    return lines.join('\n')
  }, [operation, selectedFields, whereClauses, sortClause, limit, updateField, updateValue, insertCount])

  const executeQuery = useCallback(() => {
    if (operation === 'select') {
      querySelect(
        selectedFields,
        toWorkerWhere(whereClauses),
        sortClause ? { field: sortClause.field, dir: sortClause.dir } : undefined,
        limit ? Number(limit) : undefined
      )
    } else if (operation === 'insert') {
      queryInsert(Number(insertCount))
    } else if (operation === 'update') {
      const val = getFieldType(updateField) === 'number' ? Number(updateValue) : updateValue
      queryUpdate(updateField, val, toWorkerWhere(whereClauses))
    } else if (operation === 'delete') {
      if (whereClauses.length === 0) {
        return // Safety: require WHERE for DELETE
      }
      queryDelete(toWorkerWhere(whereClauses))
    }
  }, [operation, selectedFields, whereClauses, sortClause, limit, updateField, updateValue, insertCount, querySelect, queryInsert, queryUpdate, queryDelete])

  if (!ready) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] gap-4">
        <Loader2 className="w-8 h-8 animate-spin" />
        <p className="text-white/40 text-sm tracking-wider uppercase">Initializing Database (Worker)...</p>
      </div>
    )
  }

  return (
    <div className="container mx-auto px-4 py-6 sm:py-8 max-w-7xl">
      {/* Header */}
      <div className="mb-6 sm:mb-8">
        <div className="flex items-center gap-3 mb-2">
          <h1 className="text-xl sm:text-2xl font-bold tracking-tight uppercase">Query Builder</h1>
          <div className="flex items-center gap-2 px-3 py-1 text-xs tracking-widest uppercase border border-white/20 bg-white/5">
            <Cpu className="w-3 h-3" />
            WORKER
          </div>
        </div>
        <p className="text-white/40 text-xs sm:text-sm">Build and execute SQL-like queries with the Cynos API</p>
      </div>

      <div className="grid lg:grid-cols-2 gap-4 sm:gap-6">
        {/* Left: Query Builder */}
        <div className="space-y-4 sm:space-y-6 min-w-0">
          {/* Operation Select */}
          <div className="border border-white/10">
            <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3">
              <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// OPERATION</span>
            </div>
            <div className="p-3 sm:p-4">
              <div className="flex flex-wrap gap-2">
                {(['select', 'insert', 'update', 'delete'] as Operation[]).map(op => (
                  <Button
                    key={op}
                    variant={operation === op ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setOperation(op)}
                    className={cn(
                      "uppercase tracking-wider text-[10px] sm:text-xs px-2 sm:px-3",
                      operation === op && "bg-white text-black hover:bg-white/90"
                    )}
                  >
                    {op}
                  </Button>
                ))}
              </div>
              <div className="mt-2 sm:mt-3 text-[10px] sm:text-xs text-white/30">
                {stockCount.toLocaleString()} rows in table
              </div>
            </div>
          </div>

          {/* SELECT: Field Selection */}
          {operation === 'select' && (
            <div className="border border-white/10">
              <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3">
                <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// SELECT FIELDS</span>
              </div>
              <div className="p-3 sm:p-4">
                <div className="flex flex-wrap gap-1.5 sm:gap-2">
                  {STOCK_COLUMNS.map(field => (
                    <button
                      key={field}
                      onClick={() => toggleField(field)}
                      className={cn(
                        "px-1.5 sm:px-2 py-0.5 sm:py-1 text-[10px] sm:text-xs font-mono border transition-colors",
                        selectedFields.includes(field)
                          ? "border-white/40 bg-white/10 text-white"
                          : "border-white/10 text-white/40 hover:border-white/20"
                      )}
                    >
                      {field}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          )}

          {/* INSERT: Count */}
          {operation === 'insert' && (
            <div className="border border-white/10">
              <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3">
                <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// INSERT COUNT</span>
              </div>
              <div className="p-3 sm:p-4">
                <Select value={insertCount} onValueChange={setInsertCount}>
                  <SelectTrigger className="w-[120px] sm:w-[150px] uppercase text-[10px] sm:text-xs tracking-wider">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="10">10 ROWS</SelectItem>
                    <SelectItem value="100">100 ROWS</SelectItem>
                    <SelectItem value="1000">1K ROWS</SelectItem>
                    <SelectItem value="10000">10K ROWS</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          )}

          {/* UPDATE: Set Field */}
          {operation === 'update' && (
            <div className="border border-white/10">
              <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3">
                <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// SET</span>
              </div>
              <div className="p-3 sm:p-4 flex flex-wrap gap-2 sm:gap-3">
                <Select value={updateField} onValueChange={(v) => setUpdateField(v as keyof Stock)}>
                  <SelectTrigger className="w-[100px] sm:w-[120px] uppercase text-[10px] sm:text-xs tracking-wider">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {STOCK_COLUMNS.filter(f => f !== 'id').map(field => (
                      <SelectItem key={field} value={field}>{field}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <span className="text-white/40 self-center">=</span>
                <Input
                  value={updateValue}
                  onChange={(e) => setUpdateValue(e.target.value)}
                  placeholder="value"
                  className="w-[100px] sm:w-[150px] font-mono text-[10px] sm:text-xs"
                />
              </div>
            </div>
          )}

          {/* WHERE Clauses */}
          {operation !== 'insert' && (
            <div className="border border-white/10">
              <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3 flex items-center justify-between">
                <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// WHERE</span>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={addWhereClause}
                  className="h-6 px-2 text-[10px] sm:text-xs gap-1"
                >
                  <Plus className="w-3 h-3" />
                  ADD
                </Button>
              </div>
              <div className="p-3 sm:p-4 space-y-2 sm:space-y-3">
                {whereClauses.length === 0 ? (
                  <p className="text-[10px] sm:text-xs text-white/30">
                    {operation === 'delete' ? 'DELETE requires at least one WHERE clause' : 'No conditions (returns all rows)'}
                  </p>
                ) : (
                  whereClauses.map((clause, idx) => (
                    <div key={clause.id} className="flex flex-wrap items-center gap-1.5 sm:gap-2">
                      {idx > 0 && <span className="text-[10px] sm:text-xs text-white/40 w-8 sm:w-10">AND</span>}
                      {idx === 0 && <span className="w-8 sm:w-10" />}
                      <Select
                        value={clause.field}
                        onValueChange={(v) => updateWhereClause(clause.id, { field: v as keyof Stock })}
                      >
                        <SelectTrigger className="w-[100px] sm:w-[130px] text-[10px] sm:text-xs">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="min-w-[160px]">
                          {STOCK_COLUMNS.map(field => (
                            <SelectItem key={field} value={field}>
                              <span className="flex items-center justify-between w-full gap-2">
                                <span>{field}</span>
                                {isIndexedField(field) && (
                                  <span className="text-[8px] text-violet-400 opacity-60">IDX</span>
                                )}
                              </span>
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <Select
                        value={clause.operator}
                        onValueChange={(v) => updateWhereClause(clause.id, { operator: v as Operator })}
                      >
                        <SelectTrigger className="w-[50px] sm:w-[70px] text-[10px] sm:text-xs">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {OPERATORS.map(op => (
                            <SelectItem key={op.value} value={op.value}>{op.label}</SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <Input
                        value={clause.value}
                        onChange={(e) => updateWhereClause(clause.id, { value: e.target.value })}
                        className="w-[70px] sm:w-[100px] font-mono text-[10px] sm:text-xs"
                      />
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => removeWhereClause(clause.id)}
                        className="h-7 sm:h-8 w-7 sm:w-8 p-0 text-white/40 hover:text-white"
                      >
                        <Trash2 className="w-3 h-3" />
                      </Button>
                    </div>
                  ))
                )}
              </div>
            </div>
          )}

          {/* ORDER BY & LIMIT (SELECT only) */}
          {operation === 'select' && (
            <div className="border border-white/10">
              <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3">
                <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// ORDER BY & LIMIT</span>
              </div>
              <div className="p-3 sm:p-4 flex flex-wrap gap-2 sm:gap-3">
                <Select
                  value={sortClause?.field || '_none'}
                  onValueChange={(v) => {
                    if (v === '_none') {
                      setSortClause(null)
                    } else {
                      setSortClause({ field: v as keyof Stock, dir: sortClause?.dir || 'Asc' })
                    }
                  }}
                >
                  <SelectTrigger className="w-[100px] sm:w-[120px] text-[10px] sm:text-xs">
                    <SelectValue placeholder="ORDER BY" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="_none">NO ORDER</SelectItem>
                    {STOCK_COLUMNS.map(field => (
                      <SelectItem key={field} value={field}>{field}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {sortClause && (
                  <Select
                    value={sortClause.dir}
                    onValueChange={(v) => setSortClause({ ...sortClause, dir: v as SortDir })}
                  >
                    <SelectTrigger className="w-[60px] sm:w-[80px] text-[10px] sm:text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="Asc">ASC</SelectItem>
                      <SelectItem value="Desc">DESC</SelectItem>
                    </SelectContent>
                  </Select>
                )}
                <Input
                  value={limit}
                  onChange={(e) => setLimit(e.target.value)}
                  placeholder="LIMIT"
                  className="w-[60px] sm:w-[80px] font-mono text-[10px] sm:text-xs"
                />
              </div>
            </div>
          )}

          {/* Execute Button */}
          <Button
            onClick={executeQuery}
            disabled={executing || (operation === 'delete' && whereClauses.length === 0)}
            className="w-full uppercase tracking-wider text-xs sm:text-sm gap-2 bg-white text-black hover:bg-white/90"
          >
            {executing ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                EXECUTING...
              </>
            ) : (
              <>
                <Play className="w-4 h-4" />
                EXECUTE
              </>
            )}
          </Button>
        </div>

        {/* Right: Code & Results */}
        <div className="space-y-4 sm:space-y-6 min-w-0">
          {/* Generated Code */}
          <div className="border border-white/10">
            <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3 flex items-center gap-2">
              <Code className="w-3 h-3 sm:w-4 sm:h-4 text-white/40" />
              <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// GENERATED CODE</span>
            </div>
            <div className="p-3 sm:p-4">
              <pre className="text-[10px] sm:text-xs font-mono text-white/70 whitespace-pre-wrap overflow-x-auto">
                {generateCode()}
              </pre>
            </div>
          </div>

          {/* Execution Stats */}
          {(execTime !== null || error) && (
            <div className="border border-white/10">
              <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3">
                <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// EXECUTION RESULT</span>
              </div>
              <div className="p-3 sm:p-4">
                {error ? (
                  <p className="text-[10px] sm:text-xs text-red-400">{error}</p>
                ) : (
                  <div className="flex flex-wrap gap-4 sm:gap-6 text-[10px] sm:text-xs">
                    <div className="flex items-center gap-1.5 sm:gap-2">
                      <Clock className="w-3 h-3 sm:w-4 sm:h-4 text-white/40" />
                      <span className="text-white/40">QUERY</span>
                      <span className="font-mono font-bold">{execTime?.toFixed(2)}ms</span>
                      {(latencyTime !== null || decodeTime !== null) && (
                        <span className="text-white/30 font-mono">
                          ({decodeTime !== null && `+${decodeTime.toFixed(1)}ms decode`}
                          {latencyTime !== null && decodeTime !== null && ', '}
                          {latencyTime !== null && `+${latencyTime.toFixed(1)}ms RT latency`})
                        </span>
                      )}
                    </div>
                    {affectedRows !== null && (
                      <div className="flex items-center gap-1.5 sm:gap-2">
                        <Database className="w-3 h-3 sm:w-4 sm:h-4 text-white/40" />
                        <span className="text-white/40">ROWS</span>
                        <span className="font-mono font-bold">{affectedRows.toLocaleString()}</span>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Results Table */}
          {results.length > 0 && (
            <div className="border border-white/10">
              <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3 flex items-center justify-between">
                <span className="text-[10px] sm:text-xs tracking-widest uppercase text-white/40">// RESULTS</span>
                <span className="text-[10px] sm:text-xs text-white/30">{results.length} ROWS</span>
              </div>
              <div className="max-h-[300px] sm:max-h-[400px] overflow-auto">
                <table className="w-full">
                  <thead className="sticky top-0 bg-background">
                    <tr className="border-b border-white/10">
                      {selectedFields.map(field => (
                        <th
                          key={field}
                          className="text-[8px] sm:text-[10px] uppercase tracking-wider font-normal text-white/40 whitespace-nowrap py-2 sm:py-3 px-2 sm:px-3 text-left"
                        >
                          {field}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {results.map((row, idx) => (
                      <tr key={idx} className="border-b border-white/5 hover:bg-white/[0.02]">
                        {selectedFields.map(field => (
                          <td key={field} className="font-mono text-[10px] sm:text-xs whitespace-nowrap py-1.5 sm:py-2 px-2 sm:px-3">
                            {formatValue(field, row[field])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

const formatValue = (field: keyof Stock, value: Stock[keyof Stock]) => {
  if (field === 'price' || field === 'high' || field === 'low' || field === 'open')
    return `$${(value as number).toFixed(2)}`
  if (field === 'change')
    return (value as number) >= 0 ? `+${(value as number).toFixed(2)}` : (value as number).toFixed(2)
  if (field === 'changePercent')
    return `${(value as number).toFixed(2)}%`
  if (field === 'volume' || field === 'marketCap')
    return (value as number).toLocaleString()
  if (field === 'pe')
    return (value as number).toFixed(1)
  return String(value)
}

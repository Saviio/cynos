import { useState, useEffect, useCallback } from 'react'
import { useGqlWorker } from './useGqlWorker'
import { Button } from '@/components/ui/button'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Loader2, Play, Square, Cpu, Zap, Radio } from 'lucide-react'
import { cn } from '@/lib/utils'

const EXAMPLE_QUERIES = {
  simpleQuery: {
    label: 'List Users',
    query: `query ListUsers {
  users(orderBy: [{ field: ID, direction: ASC }]) {
    id
    name
    email
    role
  }
}`,
  },
  nestedQuery: {
    label: 'Nested',
    query: `query AdminWithPosts {
  users(where: { role: { eq: "admin" } }) {
    id
    name
    posts {
      id
      title
      comments {
        text
        author { name }
      }
    }
  }
}`,
  },
  insertUser: {
    label: 'Insert User',
    query: `mutation InsertUser {
  insertUsers(input: [
    { id: 4, name: "David Lee", email: "david@example.com", role: "user" }
  ]) {
    id
    name
  }
}`,
  },
  insertPost: {
    label: 'Insert Post',
    query: `mutation InsertPost {
  insertPosts(input: [
    { id: 4, author_id: 1, title: "New Post", content: "Hello World", published: true }
  ]) {
    id
    title
    author { name }
  }
}`,
  },
  updateUser: {
    label: 'Update',
    query: `mutation UpdateUser {
  updateUsers(
    where: { id: { eq: 1 } }
    set: { name: "Alice Updated" }
  ) {
    id
    name
  }
}`,
  },
  deleteUser: {
    label: 'Delete',
    query: `mutation DeleteUser {
  deleteUsers(where: { id: { eq: 4 } }) {
    id
    name
  }
}`,
  },
}

const DEFAULT_SUBSCRIPTION = `subscription WatchUsersWithPosts {
  users(orderBy: [{ field: ID, direction: ASC }]) {
    id
    name
    role
    posts {
      id
      title
    }
  }
}`

type MobileTab = 'execute' | 'live'
type EditorTab = 'editor' | 'result' | 'schema'

export default function GraphqlDemo() {
  const {
    ready,
    schema,
    userCount,
    postCount,
    result,
    execTime,
    subscriptionData,
    error,
    executing,
    getSchema,
    execute,
    subscribe,
    unsubscribe,
    clearResult,
  } = useGqlWorker()

  const [query, setQuery] = useState(EXAMPLE_QUERIES.insertUser.query)
  const [subQuery, setSubQuery] = useState(DEFAULT_SUBSCRIPTION)
  const [subscribed, setSubscribed] = useState(false)
  const [mobileTab, setMobileTab] = useState<MobileTab>('execute')
  const [editorTab, setEditorTab] = useState<EditorTab>('editor')

  // Auto-subscribe on ready
  useEffect(() => {
    if (ready) {
      getSchema()
      subscribe(DEFAULT_SUBSCRIPTION)
      setSubscribed(true)
    }
  }, [ready, getSchema, subscribe])

  // Switch to result tab after execution
  useEffect(() => {
    if (result && !error) {
      setEditorTab('result')
    }
  }, [result, error])

  const handleExecute = useCallback(() => {
    execute(query)
  }, [execute, query])

  const handleResubscribe = useCallback(() => {
    if (subscribed) {
      unsubscribe()
    }
    subscribe(subQuery)
    setSubscribed(true)
  }, [subscribed, subscribe, unsubscribe, subQuery])

  const toggleSubscribe = useCallback(() => {
    if (subscribed) {
      unsubscribe()
      setSubscribed(false)
    } else {
      subscribe(subQuery)
      setSubscribed(true)
    }
  }, [subscribed, subscribe, unsubscribe, subQuery])

  const loadExample = useCallback((key: keyof typeof EXAMPLE_QUERIES) => {
    setQuery(EXAMPLE_QUERIES[key].query)
    setEditorTab('editor')
    clearResult()
  }, [clearResult])

  if (!ready) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] gap-4">
        <Loader2 className="w-8 h-8 animate-spin" />
        <p className="text-white/40 text-sm tracking-wider uppercase">Initializing GraphQL...</p>
      </div>
    )
  }

  // Editor Panel (inline JSX)
  const editorPanel = (
    <div className="space-y-4">
      {/* Examples */}
      <div className="flex flex-wrap gap-1.5">
        {Object.entries(EXAMPLE_QUERIES).map(([key, { label }]) => (
          <Button
            key={key}
            variant="outline"
            size="sm"
            onClick={() => loadExample(key as keyof typeof EXAMPLE_QUERIES)}
            className="text-[10px] uppercase tracking-wider h-7 px-2"
          >
            {label}
          </Button>
        ))}
      </div>

      {/* Editor with Tabs */}
      <div className="border border-white/10">
        <div className="border-b border-white/10 px-1 py-1 flex items-center justify-between">
          <Tabs value={editorTab} onValueChange={(v) => setEditorTab(v as EditorTab)}>
            <TabsList className="h-7 p-0.5 bg-transparent">
              <TabsTrigger value="editor" className="h-6 px-2 text-[10px] uppercase tracking-wider data-[state=active]:bg-white/10">
                Editor
              </TabsTrigger>
              <TabsTrigger value="result" className="h-6 px-2 text-[10px] uppercase tracking-wider data-[state=active]:bg-white/10">
                Result
                {execTime !== null && (
                  <span className="ml-1.5 text-white/40">{execTime.toFixed(2)}ms</span>
                )}
              </TabsTrigger>
              <TabsTrigger value="schema" className="h-6 px-2 text-[10px] uppercase tracking-wider data-[state=active]:bg-white/10">
                Schema
              </TabsTrigger>
            </TabsList>
          </Tabs>
        </div>

        {/* Tab Content */}
        <div className="min-h-[220px] sm:min-h-[280px]">
          {editorTab === 'editor' && (
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="w-full h-[220px] sm:h-[280px] bg-transparent p-3 font-mono text-[11px] text-white/80 resize-none focus:outline-none"
              spellCheck={false}
            />
          )}
          {editorTab === 'result' && (
            <div className="p-3 h-[220px] sm:h-[280px] overflow-auto">
              {error ? (
                <p className="text-[11px] text-red-400 font-mono">{error}</p>
              ) : result ? (
                <pre className="text-[11px] font-mono text-white/70 whitespace-pre-wrap">
                  {JSON.stringify(result, null, 2)}
                </pre>
              ) : (
                <p className="text-[11px] text-white/30">Execute a query to see results</p>
              )}
            </div>
          )}
          {editorTab === 'schema' && (
            <div className="p-3 h-[220px] sm:h-[280px] overflow-auto">
              {schema ? (
                <pre className="text-[10px] font-mono text-white/50 whitespace-pre">{schema}</pre>
              ) : (
                <p className="text-[11px] text-white/30">Loading schema...</p>
              )}
            </div>
          )}
        </div>

        {/* Execute Button */}
        <div className="border-t border-white/10 p-2">
          <Button
            onClick={handleExecute}
            disabled={executing || !query.trim()}
            size="sm"
            className="w-full uppercase tracking-wider text-[10px] gap-2 bg-white text-black hover:bg-white/90 h-8"
          >
            {executing ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
            {executing ? 'EXECUTING...' : 'EXECUTE'}
          </Button>
        </div>
      </div>
    </div>
  )

  // Live Panel (inline JSX)
  const livePanel = (
    <div className="space-y-4 h-full flex flex-col">
      {/* Subscription Editor */}
      <div className="border border-white/10">
        <div className="border-b border-white/10 px-3 py-2 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Radio className="w-3 h-3 text-white/40" />
            <span className="text-[10px] tracking-widest uppercase text-white/40">SUBSCRIPTION</span>
          </div>
          {subscribed && (
            <div className="flex items-center gap-1.5 text-[10px] text-white/50">
              <span className="w-1.5 h-1.5 bg-green-400 rounded-full animate-pulse" />
              LIVE
            </div>
          )}
        </div>
        <textarea
          value={subQuery}
          onChange={(e) => setSubQuery(e.target.value)}
          className="w-full h-[140px] bg-transparent p-3 font-mono text-[11px] text-white/80 resize-none focus:outline-none"
          spellCheck={false}
        />
        <div className="border-t border-white/10 p-2 flex gap-2">
          <Button
            variant={subscribed ? 'default' : 'outline'}
            size="sm"
            onClick={toggleSubscribe}
            className={cn(
              "flex-1 h-8 uppercase tracking-wider text-[10px] gap-1.5",
              subscribed && "bg-white text-black hover:bg-white/90"
            )}
          >
            {subscribed ? <Square className="w-3 h-3" /> : <Zap className="w-3 h-3" />}
            {subscribed ? 'STOP' : 'START'}
          </Button>
          {subscribed && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleResubscribe}
              className="h-8 uppercase tracking-wider text-[10px] gap-1.5"
            >
              <Play className="w-3 h-3" />
              APPLY
            </Button>
          )}
        </div>
      </div>

      {/* Live Data */}
      <div className="border border-white/20 flex-1 min-h-[200px] flex flex-col">
        <div className="border-b border-white/10 px-3 py-2">
          <span className="text-[10px] tracking-widest uppercase text-white/40">LIVE DATA</span>
        </div>
        <div className="p-3 flex-1 overflow-auto">
          {subscriptionData ? (
            <pre className="text-[11px] font-mono text-white/70 whitespace-pre-wrap">
              {JSON.stringify(subscriptionData, null, 2)}
            </pre>
          ) : (
            <p className="text-[11px] text-white/30">
              {subscribed ? 'Waiting for data...' : 'Click START to subscribe'}
            </p>
          )}
        </div>
      </div>
    </div>
  )

  return (
    <div className="container mx-auto px-4 py-6 sm:py-8 max-w-6xl">
      {/* Header */}
      <div className="mb-6 sm:mb-8">
        <div className="flex items-center gap-3 mb-2">
          <h1 className="text-xl sm:text-2xl font-bold tracking-tight uppercase">GraphQL Playground</h1>
          <div className="flex items-center gap-2 px-3 py-1 text-xs tracking-widest uppercase border border-white/20 bg-white/5">
            <Cpu className="w-3 h-3" />
            WORKER
          </div>
        </div>
        <p className="text-white/40 text-xs sm:text-sm">Auto-generated schema with queries, mutations & live subscriptions</p>
      </div>

      {/* Stats Bar */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-px mb-6 sm:mb-8 bg-white/10">
        <div className="bg-background p-3 sm:p-4">
          <div className="text-[10px] sm:text-xs text-white/40 uppercase tracking-wider mb-1">Users</div>
          <div className="text-lg sm:text-xl font-mono">{userCount}</div>
        </div>
        <div className="bg-background p-3 sm:p-4">
          <div className="text-[10px] sm:text-xs text-white/40 uppercase tracking-wider mb-1">Posts</div>
          <div className="text-lg sm:text-xl font-mono">{postCount}</div>
        </div>
        <div className="bg-background p-3 sm:p-4">
          <div className="text-[10px] sm:text-xs text-white/40 uppercase tracking-wider mb-1">Tables</div>
          <div className="text-lg sm:text-xl font-mono">3</div>
        </div>
        <div className="bg-background p-3 sm:p-4">
          <div className="text-[10px] sm:text-xs text-white/40 uppercase tracking-wider mb-1">Relations</div>
          <div className="text-lg sm:text-xl font-mono">3</div>
        </div>
      </div>

      {/* Mobile Tabs */}
      <div className="lg:hidden mb-4">
        <Tabs value={mobileTab} onValueChange={(v) => setMobileTab(v as MobileTab)}>
          <TabsList className="w-full grid grid-cols-2">
            <TabsTrigger value="execute" className="gap-1.5 text-xs">
              Execute
            </TabsTrigger>
            <TabsTrigger value="live" className="gap-1.5 text-xs">
              Live
              {subscribed && <span className="w-1.5 h-1.5 bg-green-400 rounded-full animate-pulse" />}
            </TabsTrigger>
          </TabsList>
        </Tabs>
      </div>

      {/* Desktop: Side by Side */}
      <div className="hidden lg:grid lg:grid-cols-2 gap-6">
        {editorPanel}
        {livePanel}
      </div>

      {/* Mobile: Tab Content */}
      <div className="lg:hidden">
        {mobileTab === 'execute' ? editorPanel : livePanel}
      </div>
    </div>
  )
}

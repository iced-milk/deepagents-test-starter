import { useState, useRef, useCallback } from 'react';

// ─── Route definitions ───

interface RouteConfig {
  path: string;
  label: string;
  description: string;
  type: 'json' | 'sse';
  defaultMessage: string;
}

const ROUTES: RouteConfig[] = [
  {
    path: '/chat',
    label: '/chat',
    description: '同步聊天 — invoke() 返回完整 JSON 响应',
    type: 'json',
    defaultMessage: '你好，1+1 等于几？',
  },
  {
    path: '/stream',
    label: '/stream',
    description: '流式聊天 — SSE 逐 token 输出 ai 响应',
    type: 'sse',
    defaultMessage: '用三句话解释 React Hooks。',
  },
  {
    path: '/tool',
    label: '/tool',
    description: '流式聊天 + 搜索工具调用 — SSE 输出含 tool_call / tool_result 事件',
    type: 'sse',
    defaultMessage: '今天科技圈有什么新闻？',
  },
  {
    path: '/subagent',
    label: '/subagent',
    description: '流式聊天 + subagent 委派 — SSE 输出含 subagent 生命周期事件（pending → running → complete）',
    type: 'sse',
    defaultMessage: '帮我调研 TypeScript 和 JavaScript 的主要区别。',
  },
];

// ─── Output line types ───

interface OutputLine {
  id: number;
  kind: 'text' | 'event' | 'error' | 'status';
  tag?: string;
  tagClass?: string;
  content: string;
}

let lineId = 0;

// ─── App ───

export default function App() {
  const [activeIdx, setActiveIdx] = useState(0);
  const [message, setMessage] = useState(ROUTES[0].defaultMessage);
  const [lines, setLines] = useState<OutputLine[]>([]);
  const [loading, setLoading] = useState(false);
  const outputRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  // 用户是否贴在底部；只有贴底时新内容才自动滚动，避免用户上翻被强制拉回。
  const stickToBottomRef = useRef(true);

  const route = ROUTES[activeIdx];

  const scrollToBottom = useCallback(() => {
    if (!stickToBottomRef.current) return;
    requestAnimationFrame(() => {
      const el = outputRef.current;
      if (el) el.scrollTop = el.scrollHeight;
    });
  }, []);

  // 监听滚动：根据当前位置决定是否继续自动贴底。
  // 距底 <= 8px 视为贴底（容忍浮点/亚像素误差及不同 DPR 的舍入）。
  const handleOutputScroll = useCallback(() => {
    const el = outputRef.current;
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    stickToBottomRef.current = distanceFromBottom <= 8;
  }, []);

  const appendLine = useCallback((line: Omit<OutputLine, 'id'>) => {
    setLines(prev => [...prev, { ...line, id: ++lineId }]);
    scrollToBottom();
  }, [scrollToBottom]);

  // Append text content to the last "text" line (for streaming tokens)
  const appendToLastText = useCallback((text: string) => {
    setLines(prev => {
      const last = prev[prev.length - 1];
      if (last && last.kind === 'text') {
        return [...prev.slice(0, -1), { ...last, content: last.content + text }];
      }
      return [...prev, { id: ++lineId, kind: 'text', content: text }];
    });
    scrollToBottom();
  }, [scrollToBottom]);

  const switchRoute = (idx: number) => {
    if (loading) return;
    setActiveIdx(idx);
    setMessage(ROUTES[idx].defaultMessage);
    setLines([]);
  };

  const handleStop = useCallback(async () => {
    try {
      await fetch('/stop', { method: 'POST' });
    } catch {
      // 忽略 stop 请求失败
    }
    // 前端 abort fetch 连接（断开 SSE 读取 / JSON 等待）
    abortRef.current?.abort();
    // 显示提示
    appendLine({ kind: 'status', tag: 'STOPPED', tagClass: 'stopped', content: 'Request aborted by user.' });
  }, [appendLine]);

  const handleClear = () => {
    setLines([]);
    stickToBottomRef.current = true;
  };

  // ─── Send Request ───

  const handleSend = async () => {
    if (loading || !message.trim()) return;

    setLines([]);
    setLoading(true);
    // 新一轮请求：重置为贴底，恢复自动滚动。
    stickToBottomRef.current = true;

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      if (route.type === 'json') {
        await sendJsonRequest(route, message, controller.signal, appendLine);
      } else {
        await sendSSERequest(route, message, controller.signal, appendLine, appendToLastText);
      }
    } catch (err: unknown) {
      if ((err as Error).name !== 'AbortError') {
        appendLine({ kind: 'error', tag: 'ERROR', tagClass: 'error', content: (err as Error).message });
      }
    } finally {
      setLoading(false);
      abortRef.current = null;
    }
  };

  return (
    <div className="app">
      {/* Header */}
      <div className="header">
        <h1><span>Deep Agents</span> Test Console <small className="runtime-tag">Node.js</small></h1>
      </div>

      {/* Tabs */}
      <div className="tabs">
        {ROUTES.map((r, i) => (
          <button
            key={r.path}
            className={`tab ${i === activeIdx ? 'active' : ''}`}
            onClick={() => switchRoute(i)}
          >
            {r.label}
          </button>
        ))}
      </div>

      {/* Route Info */}
      <div className="route-info">
        <span className="method post">POST</span>
        <code>{route.path}</code>
        {' — '}{route.description}
      </div>

      {/* Input */}
      <div className="input-area">
        <input
          value={message}
          onChange={e => setMessage(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && !loading && handleSend()}
          placeholder="Enter message..."
          disabled={loading}
        />
        {loading ? (
          <button className="btn-stop" onClick={handleStop}>
            Stop
          </button>
        ) : (
          <button className="btn-send" onClick={handleSend} disabled={!message.trim()}>
            Send
          </button>
        )}
      </div>

      {/* Output */}
      <div className="output-area">
        <div className="output-header">
          <span>
            Response
            {loading && <span className="loading-dot" />}
          </span>
          <button className="btn-clear" onClick={handleClear}>
            Clear
          </button>
        </div>
        <div className="output-box" ref={outputRef} onScroll={handleOutputScroll}>
          {lines.map(line => renderLine(line))}
        </div>
      </div>
    </div>
  );
}

// ─── Render a single output line ───

function renderLine(line: OutputLine) {
  if (line.kind === 'text') {
    return <span key={line.id}>{line.content}</span>;
  }
  if (line.kind === 'event' || line.kind === 'error') {
    return (
      <span key={line.id} className="status-line">
        <span className={`event-tag ${line.tagClass ?? ''}`}>{line.tag}</span>
        {line.content}
        {'\n'}
      </span>
    );
  }
  if (line.kind === 'status') {
    return (
      <span key={line.id} className="status-line">
        {line.content}{'\n'}
      </span>
    );
  }
  return null;
}

// ─── JSON request (chat) ───

async function sendJsonRequest(
  route: RouteConfig,
  message: string,
  signal: AbortSignal,
  appendLine: (line: Omit<OutputLine, 'id'>) => void,
) {
  const res = await fetch(route.path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message }),
    signal,
  });
  const data = await res.json();
  appendLine({ kind: 'text', content: data.response ?? JSON.stringify(data, null, 2) });
}

// ─── SSE request (stream, tool, subagent) ───

async function sendSSERequest(
  route: RouteConfig,
  message: string,
  signal: AbortSignal,
  appendLine: (line: Omit<OutputLine, 'id'>) => void,
  appendToLastText: (text: string) => void,
) {
  const res = await fetch(route.path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message }),
    signal,
  });

  if (!res.ok || !res.body) {
    throw new Error(`HTTP ${res.status}: ${res.statusText}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  // Track which "source" we're currently appending text to, so that a source_switch
  // forces a new text line.
  let currentTextSource = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const parts = buffer.split('\n\n');
    buffer = parts.pop() ?? '';

    for (const part of parts) {
      const trimmed = part.trim();
      if (!trimmed) continue;

      // SSE 注释行，按协议忽略，不展示到 UI。
      if (trimmed.startsWith(':')) continue;

      const line = trimmed.replace(/^data:\s*/, '');
      if (!line || line === '[DONE]') {
        if (line === '[DONE]') {
          appendLine({ kind: 'status', content: '\n✓ Stream complete' });
        }
        continue;
      }

      let event: Record<string, unknown>;
      try {
        event = JSON.parse(line);
      } catch {
        appendToLastText(line);
        continue;
      }

      const type = event.type as string;

      switch (type) {
        case 'ping': {
          // 心跳帧：后端每 5s 空闲时发 {"type":"ping","ts":...}，用于保活。
          // 静默丢弃，不渲染到 UI。
          break;
        }

        case 'ai_response': {
          // If the source changed (e.g. main vs subagent), start a new text line
          const source = (event.agent as string) ?? 'main';
          if (source !== currentTextSource) {
            currentTextSource = source;
            appendLine({ kind: 'text', content: '' }); // force new text line
          }
          appendToLastText(event.content as string);
          break;
        }

        case 'source_switch': {
          const agent = (event.agent as string) ?? 'unknown';
          currentTextSource = agent;
          appendLine({ kind: 'text', content: '' }); // force new line segment
          appendLine({
            kind: 'event',
            tag: '⇢ ' + agent,
            tagClass: 'source',
            content: event.namespace ? `(${event.namespace})` : '',
          });
          break;
        }

        case 'subagent_lifecycle': {
          const status = event.status as string;
          const agent = (event.agent as string) ?? '';
          let info = `${agent}`;
          if (event.description) info += ` — ${event.description}`;
          if (status === 'complete' && event.content) {
            info += `\n   Result: ${(event.content as string).slice(0, 200)}...`;
          }
          appendLine({
            kind: 'event',
            tag: status.toUpperCase(),
            tagClass: 'lifecycle',
            content: info,
          });
          break;
        }

        case 'tool_call': {
          appendLine({
            kind: 'event',
            tag: 'TOOL',
            tagClass: 'tool',
            content: `Calling ${event.name}...`,
          });
          break;
        }

        case 'tool_result': {
          const preview = ((event.content as string) ?? '').slice(0, 300);
          appendLine({
            kind: 'event',
            tag: 'RESULT',
            tagClass: 'tool',
            content: `${event.name}: ${preview}`,
          });
          break;
        }

        case 'error_message': {
          appendLine({
            kind: 'error',
            tag: 'ERROR',
            tagClass: 'error',
            content: event.content as string,
          });
          break;
        }

        default: {
          appendLine({ kind: 'status', content: `[${type}] ${JSON.stringify(event)}` });
        }
      }
    }
  }
}

import { initChatModel, AIMessageChunk, ToolMessage, tool } from 'langchain';
import { modelRetryMiddleware, modelCallLimitMiddleware, toolRetryMiddleware, toolCallLimitMiddleware } from 'langchain';
import { createDeepAgent } from 'deepagents';
import { DDGS, type SearchResult } from '@phukon/duckduckgo-search';
import { z } from 'zod';

type Model = Awaited<ReturnType<typeof initChatModel>>;
type Agent = ReturnType<typeof createDeepAgent>;

interface Env {
    AI_GATEWAY_API_KEY: string;
    AI_GATEWAY_BASE_URL: string;
}

// ─── Unified logger with [tool][timestamp] prefix ───
const logger = {
    log(...args: unknown[]) {
        console.log(`[tool][${new Date().toISOString()}]`, ...args);
    },
    error(...args: unknown[]) {
        console.error(`[tool][${new Date().toISOString()}]`, ...args);
    },
};

const SYSTEM_PROMPT = `You are a helpful assistant. Today's date is ${new Date().toISOString().slice(0, 10)}. Use \`internet_search\` to look up information before answering. When searching, prefer including the current year or recent time range to get the latest results. Answer concisely.`;

let model: Model | null = null;
let agent: Agent | null = null;

// Internet search tool

const ddgs = new DDGS({ timeout: 15000 });

const internetSearch = tool(
    async ({ query, maxResults = 3 }: { query: string; maxResults?: number }) => {
        const results: SearchResult[] = await ddgs.text({
            keywords: query,
            maxResults,
        });
        if (!results || results.length === 0) {
            return 'No search results found.';
        }
        return results
            .map((r, i) => `[${i + 1}] ${r.title}\n${r.href}\n${r.body ?? ''}`)
            .join('\n\n');
    },
    {
        name: 'internet_search',
        description: 'Search the internet using DuckDuckGo. Returns titles, URLs, and snippets for the given query.',
        schema: z.object({
            query: z.string().describe('The search query'),
            maxResults: z.number().optional().default(3).describe('Maximum number of results to return'),
        }),
    }
);

function getEnv(contextEnv: Record<string, string | undefined> | undefined): Env {
    const source = contextEnv ?? {};
    const required = ['AI_GATEWAY_API_KEY', 'AI_GATEWAY_BASE_URL'] as const;
    const missing = required.filter((k) => !source[k]?.trim());

    if (missing.length) {
        throw new Error(`Missing environment variables: ${missing.join(', ')}`);
    }

    return {
        AI_GATEWAY_API_KEY: source.AI_GATEWAY_API_KEY!,
        AI_GATEWAY_BASE_URL: source.AI_GATEWAY_BASE_URL!,
    };
}

async function getModel(env: Env) {
    if (!model) {
        logger.log('Initializing model...');
        model = await initChatModel('@Pages/deepseek-v4-flash', {
            modelProvider: 'openai',
            apiKey: env.AI_GATEWAY_API_KEY,
            configuration: {
                baseURL: env.AI_GATEWAY_BASE_URL,
            },
            temperature: 0,
            timeout: 300_000,
        });
    }
    return model;
}

function getAgent(modelInstance: Model) {
    if (!agent) {
        logger.log('Initializing agent...');
        agent = createDeepAgent({
            model: modelInstance,
            systemPrompt: SYSTEM_PROMPT,
            tools: [internetSearch],
            middleware: [
                modelRetryMiddleware({ maxRetries: 3 }),
                modelCallLimitMiddleware({ runLimit: 30 }),
                toolRetryMiddleware({ maxRetries: 2, tools: ['internet_search'] }),
                toolCallLimitMiddleware({ toolName: 'internet_search', runLimit: 15 }),
            ],
        });
    }
    return agent;
}

/**
 * Async generator that yields SSE-formatted AI content tokens.
 */
async function* eventStream(agentInstance: Agent, userMessage: string, signal?: AbortSignal): AsyncGenerator<string> {
    try {
        logger.log(`starting stream for message: "${userMessage.slice(0, 80)}"`);
        const stream = await agentInstance.stream(
            { messages: [{ role: "user", content: userMessage }] },
            { streamMode: "messages", signal }
        );

        let lastTickAt = Date.now();
        let lastChunkKind: 'tool_call' | 'tool_result' | 'ai_response' | 'other' = 'other';
        const GAP_THRESHOLD_MS = 3000;

        for await (const chunk of stream) {
            const gap = Date.now() - lastTickAt;
            if (gap > GAP_THRESHOLD_MS) {
                // Annotate which chunk kind preceded the gap, so we can quickly locate stalls
                // (a common case: tool_call → tool_result delay caused by DDG search latency).
                logger.log(`[gap] ${gap}ms before next chunk (after=${lastChunkKind})`);
            }
            lastTickAt = Date.now();

            if (signal?.aborted) break;
            const [message] = chunk;

            // Streaming tool calls
            if (AIMessageChunk.isInstance(message) && message.tool_call_chunks?.length) {
                lastChunkKind = 'tool_call';
                for (const tc of message.tool_call_chunks) {
                    if (tc.name) {
                        logger.log(`tool call: ${tc.name}`);
                        yield `data: ${JSON.stringify({ type: 'tool_call', name: tc.name })}\n\n`;
                    }
                    if (tc.args) {
                        logger.log(`tool call args: ${tc.args}`);
                    }
                }
                continue;
            }

            // Tool results
            if (ToolMessage.isInstance(message)) {
                lastChunkKind = 'tool_result';
                logger.log(`tool result [${message.name}]: ${message.text?.slice(0, 150)}`);
                yield `data: ${JSON.stringify({ type: 'tool_result', name: message.name, content: message.text?.slice(0, 500) })}\n\n`;
                continue;
            }

            // AI text response
            if (AIMessageChunk.isInstance(message) && message.text) {
                lastChunkKind = 'ai_response';
                const cleaned = message.text.replace(/\n{3,}/g, '\n\n');
                if (cleaned) {
                    logger.log('ai response:', cleaned);
                    yield `data: ${JSON.stringify({ type: 'ai_response', content: cleaned })}\n\n`;
                }
            }
        }
        logger.log('stream completed');
    } catch (e: unknown) {
        const error = e as Error;
        if (error.name === 'AbortError' || signal?.aborted) {
            logger.log('aborted by user');
        } else {
            logger.error('error:', error.message, error.stack);
            yield `data: ${JSON.stringify({ type: 'error_message', content: `Stream error: ${error.message}` })}\n\n`;
        }
    }

    yield "data: [DONE]\n\n";
}

export async function onRequest(context: any) {
    const { request, env, conversation_id: conversationId } = context;

    const { message } = request?.body ?? {};
    logger.log('user message:', message);
    if (!message) {
        logger.error('Missing chat message');
        return new Response('Missing chat message', { status: 400 });
    }

    const signal = request?.signal as AbortSignal | undefined;

    let agentInstance: Agent;
    try {
        const envVars = getEnv(env);
        const modelInstance = await getModel(envVars);
        agentInstance = getAgent(modelInstance);
    } catch (e) {
        const msg = (e as Error).message;
        logger.error(msg);
        return new Response(JSON.stringify({ error: msg }), {
            status: 500,
            headers: { 'Content-Type': 'application/json; charset=UTF-8' },
        });
    }

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
        async start(controller) {
            const HEARTBEAT_INTERVAL_MS = 5_000;

            // Session frame: emit conversationId first so the client can save it
            if (conversationId) {
                const sessionFrame = `data: ${JSON.stringify({ type: 'session', conversationId })}\n\n`;
                controller.enqueue(encoder.encode(sessionFrame));
            }

            // Heartbeat: emit a JSON data frame {"type":"ping","ts":<ms>} every 5s
            // to keep intermediaries and clients from closing an idle connection.
            // Frontend filters these via `case 'ping'` and does not render them.
            const heartbeat = setInterval(() => {
                try {
                    const frame = `data: ${JSON.stringify({ type: 'ping', ts: Date.now() })}\n\n`;
                    controller.enqueue(encoder.encode(frame));
                } catch {
                    /* controller already closed */
                }
            }, HEARTBEAT_INTERVAL_MS);

            try {
                for await (const chunk of eventStream(agentInstance, message, signal)) {
                    if (signal?.aborted) break;
                    controller.enqueue(encoder.encode(chunk));
                }
            } catch (e) {
                const error = e as Error;
                if (error.name === 'AbortError' || signal?.aborted) return;
                const errorEvent = `data: ${JSON.stringify({ type: "error_message", content: error.message, source: "main", node: "system" })}\n\n`;
                controller.enqueue(encoder.encode(errorEvent));
            } finally {
                clearInterval(heartbeat);
                controller.close();
            }
        },
        cancel() {
            logger.log('client disconnected');
        },
    });

    return new Response(stream, {
        status: 200,
        headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
        },
    });
}

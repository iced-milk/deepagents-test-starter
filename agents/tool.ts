import { initChatModel, AIMessageChunk, ToolMessage, tool } from 'langchain';
import { modelRetryMiddleware, modelCallLimitMiddleware, toolRetryMiddleware, toolCallLimitMiddleware } from 'langchain';
import { createDeepAgent } from 'deepagents';
import { DDGS, type SearchResult } from '@phukon/duckduckgo-search';
import { z } from 'zod';

type Model = Awaited<ReturnType<typeof initChatModel>>;
type Agent = ReturnType<typeof createDeepAgent>;

interface EnvConfig {
    LLM_MODEL: string;
    LLM_API_KEY: string;
    LLM_BASE_URL: string;
    [KEY: string]: string;
}

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

const SYSTEM_PROMPT = `You are a helpful assistant. Today's date is ${new Date().toISOString().slice(0, 10)}. Use \`internet_search\` to look up information before answering. When searching, prefer including the current year or recent time range to get the latest results. Answer concisely.`;

async function getModel(env: EnvConfig) {
    if (!model) {
        console.log('Initializing model...');
        model = await initChatModel(env.LLM_MODEL, {
            modelProvider: "openai",
            apiKey: env.LLM_API_KEY,
            configuration: {
                baseURL: env.LLM_BASE_URL,
            },
            temperature: 0,
            timeout: 300_000,
        });
    }
    return model;
}

function getAgent(modelInstance: Model) {
    if (!agent) {
        console.log('Initializing agent...');
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
async function* eventStream(agentInstance: Agent, userMessage: string): AsyncGenerator<string> {
    try {
        const stream = await agentInstance.stream(
            { messages: [{ role: "user", content: userMessage }] },
            { streamMode: "messages" }
        );

        for await (const chunk of stream) {
            const [message] = chunk;

            // Streaming tool calls
            if (AIMessageChunk.isInstance(message) && message.tool_call_chunks?.length) {
                for (const tc of message.tool_call_chunks) {
                    if (tc.name) {
                        console.log(`[tool] tool call: ${tc.name}`);
                        yield `data: ${JSON.stringify({ type: 'tool_call', name: tc.name })}\n\n`;
                    }
                    if (tc.args) {
                        console.log(`[tool] tool call args: ${tc.args}`);
                    }
                }
                continue;
            }

            // Tool results
            if (ToolMessage.isInstance(message)) {
                console.log(`[tool] tool result [${message.name}]: ${message.text?.slice(0, 150)}`);
                yield `data: ${JSON.stringify({ type: 'tool_result', name: message.name, content: message.text?.slice(0, 500) })}\n\n`;
                continue;
            }

            // AI text response
            if (AIMessageChunk.isInstance(message) && message.text) {
                const cleaned = message.text.replace(/\n{3,}/g, '\n\n');
                if (cleaned) {
                    console.log('[tool] ai response:', cleaned);
                    yield `data: ${JSON.stringify({ type: 'ai_response', content: cleaned })}\n\n`;
                }
            }
        }
    } catch (e: unknown) {
        const error = e as Error;
        console.error('[tool] error:', error.message, error.stack);
        yield `data: ${JSON.stringify({ type: 'error_message', content: `Stream error: ${error.message}` })}\n\n`;
    }

    yield "data: [DONE]\n\n";
}

export async function onRequest(context: any) {
    const { env, request } = context;

    const { LLM_MODEL, LLM_API_KEY, LLM_BASE_URL } = env;
    if (!LLM_MODEL || !LLM_API_KEY || !LLM_BASE_URL) {
        console.error('Missing environment variables');
        return new Response('Missing environment variables', { status: 500 });
    }

    const modelInstance = await getModel({ LLM_MODEL, LLM_API_KEY, LLM_BASE_URL });
    const agentInstance = getAgent(modelInstance);

    const { message } = request?.body ?? {};
    console.log('[tool] user message:', message);
    if (!message) {
        console.error('Missing chat message');
        return new Response('Missing chat message', { status: 400 });
    }

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
        async start(controller) {
            try {
                for await (const chunk of eventStream(agentInstance, message)) {
                    controller.enqueue(encoder.encode(chunk));
                }
            } catch (e) {
                const error = e as Error;
                const errorEvent = `data: ${JSON.stringify({ type: "error_message", content: error.message, source: "main", node: "system" })}\n\n`;
                controller.enqueue(encoder.encode(errorEvent));
            } finally {
                controller.close();
            }
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

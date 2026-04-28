import { initChatModel, AIMessageChunk } from 'langchain';
import { modelRetryMiddleware, modelCallLimitMiddleware } from 'langchain';
import { createDeepAgent } from 'deepagents';

type Model = Awaited<ReturnType<typeof initChatModel>>;
type Agent = ReturnType<typeof createDeepAgent>;

interface Env {
    AI_GATEWAY_API_KEY: string;
    AI_GATEWAY_BASE_URL: string;
}

// ─── Unified logger with [stream][timestamp] prefix ───
const logger = {
    log(...args: unknown[]) {
        console.log(`[stream][${new Date().toISOString()}]`, ...args);
    },
    error(...args: unknown[]) {
        console.error(`[stream][${new Date().toISOString()}]`, ...args);
    },
};

const SYSTEM_PROMPT = 'You are a helpful assistant. Answer questions concisely and clearly.';

let model: Model | null = null;
let agent: Agent | null = null;

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
            middleware: [
                modelRetryMiddleware({ maxRetries: 3 }),
                modelCallLimitMiddleware({ runLimit: 30 }),
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
            { streamMode: "messages" }
        );

        let lastTickAt = Date.now();
        const GAP_THRESHOLD_MS = 3000;

        for await (const chunk of stream) {
            const gap = Date.now() - lastTickAt;
            if (gap > GAP_THRESHOLD_MS) {
                logger.log(`[gap] ${gap}ms before next chunk`);
            }
            lastTickAt = Date.now();

            if (signal?.aborted) break;
            const [message] = chunk;

            if (AIMessageChunk.isInstance(message) && message.text) {
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
            // Triggered when the client disconnects
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

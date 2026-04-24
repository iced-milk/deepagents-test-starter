import { initChatModel, AIMessageChunk } from 'langchain';
import { modelRetryMiddleware, modelCallLimitMiddleware } from 'langchain';
import { createDeepAgent } from 'deepagents';

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

const SYSTEM_PROMPT = 'You are a helpful assistant. Answer questions concisely and clearly.';

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
async function* eventStream(agentInstance: Agent, userMessage: string): AsyncGenerator<string> {
    try {
        const stream = await agentInstance.stream(
            { messages: [{ role: "user", content: userMessage }] },
            { streamMode: "messages" }
        );

        for await (const chunk of stream) {
            const [message] = chunk;

            if (AIMessageChunk.isInstance(message) && message.text) {
                const cleaned = message.text.replace(/\n{3,}/g, '\n\n');
                if (cleaned) {
                    console.log('[stream] ai response:', cleaned);
                    yield `data: ${JSON.stringify({ type: 'ai_response', content: cleaned })}\n\n`;
                }
            }
        }
    } catch (e: unknown) {
        const error = e as Error;
        console.error('[stream] error:', error.message, error.stack);
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
    console.log('[stream] user message:', message);
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

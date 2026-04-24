import { initChatModel } from 'langchain';
import { modelRetryMiddleware, modelCallLimitMiddleware } from 'langchain';
import { createDeepAgent } from 'deepagents';

type Model = Awaited<ReturnType<typeof initChatModel>>;
type Agent = ReturnType<typeof createDeepAgent>;

// ─── Unified logger with [chat][timestamp] prefix ───

const logger = {
    log(...args: unknown[]) {
        console.log(`[chat][${new Date().toISOString()}]`, ...args);
    },
    error(...args: unknown[]) {
        console.error(`[chat][${new Date().toISOString()}]`, ...args);
    },
};

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
        logger.log('Initializing model...');
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

export async function onRequest(context: any) {
    const { env, request } = context;

    const { LLM_MODEL, LLM_API_KEY, LLM_BASE_URL } = env;
    if (!LLM_MODEL || !LLM_API_KEY || !LLM_BASE_URL) {
        logger.error('Missing environment variables');
        return new Response('Missing environment variables', { status: 500 });
    }

    const modelInstance = await getModel({ LLM_MODEL, LLM_API_KEY, LLM_BASE_URL });
    const agentInstance = getAgent(modelInstance);

    const { message: userMessage } = request?.body ?? {};
    logger.log('user message:', userMessage);
    if (!userMessage) {
        logger.error('Missing chat message');
        return new Response('Missing chat message', { status: 400 });
    }

    const signal = request?.signal as AbortSignal | undefined;
    try {
        const result = await agentInstance.invoke(
            { messages: [{ role: "user", content: userMessage }] },
            { signal },
        );
        const messages = (result as any).messages;
        logger.log('ai:', messages[messages.length - 1].content);

        return new Response(JSON.stringify({ response: messages[messages.length - 1].content }), {
            status: 200,
            headers: { 'Content-Type': 'application/json; charset=UTF-8' },
        });
    } catch (e: unknown) {
        const error = e as Error;
        // Re-throw AbortError so the Runtime returns 499
        if (error.name === 'AbortError' || signal?.aborted) {
            logger.log('aborted by user');
            throw error;
        }
        logger.error('error:', error.message);
        return new Response(JSON.stringify({ error: error.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json; charset=UTF-8' },
        });
    }
}


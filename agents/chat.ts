import { initChatModel } from 'langchain';
import { modelRetryMiddleware, modelCallLimitMiddleware } from 'langchain';
import { createDeepAgent } from 'deepagents';

type Model = Awaited<ReturnType<typeof initChatModel>>;
type Agent = ReturnType<typeof createDeepAgent>;

interface Env {
    AI_GATEWAY_API_KEY: string;
    AI_GATEWAY_BASE_URL: string;
}

// ─── Unified logger with [chat][timestamp] prefix ───
const logger = {
    log(...args: unknown[]) {
        console.log(`[chat][${new Date().toISOString()}]`, ...args);
    },
    error(...args: unknown[]) {
        console.error(`[chat][${new Date().toISOString()}]`, ...args);
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

export async function onRequest(context: any) {
    const { request, env } = context;

    const { message: userMessage } = request?.body ?? {};
    logger.log('user message:', userMessage);
    if (!userMessage) {
        logger.error('Missing chat message');
        return new Response('Missing chat message', { status: 400 });
    }

    const signal = request?.signal as AbortSignal | undefined;
    try {
        const envVars = getEnv(env);
        const modelInstance = await getModel(envVars);
        const agentInstance = getAgent(modelInstance);

        const result = await agentInstance.invoke(
            { messages: [{ role: "user", content: userMessage }] },
            { signal },
        );
        const messages = (result as any).messages;
        logger.log('ai:', messages[messages.length - 1].content);

        return new Response(JSON.stringify({
            response: messages[messages.length - 1].content,
        }), {
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


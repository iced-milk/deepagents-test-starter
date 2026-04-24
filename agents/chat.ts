import { initChatModel } from 'langchain';
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

export async function onRequest(context: any) {
    const { env, request } = context;

    const { LLM_MODEL, LLM_API_KEY, LLM_BASE_URL } = env;
    if (!LLM_MODEL || !LLM_API_KEY || !LLM_BASE_URL) {
        console.error('Missing environment variables');
        return new Response('Missing environment variables', { status: 500 });
    }

    const modelInstance = await getModel({ LLM_MODEL, LLM_API_KEY, LLM_BASE_URL });
    const agentInstance = getAgent(modelInstance);

    const { message: userMessage } = request?.body ?? {};
    console.log('[chat] user message:', userMessage);
    if (!userMessage) {
        console.error('Missing chat message');
        return new Response('Missing chat message', { status: 400 });
    }

    const result = await agentInstance.invoke({
        messages: [{ role: "user", content: userMessage }],
    });
    const messages = (result as any).messages;
    console.log('[chat] ai:', messages[messages.length - 1].content);

    return new Response(JSON.stringify({ response: messages[messages.length - 1].content }), {
        status: 200,
        headers: { 'Content-Type': 'application/json; charset=UTF-8' },
    });
}


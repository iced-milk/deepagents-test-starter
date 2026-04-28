import { initChatModel } from 'langchain';
import { modelRetryMiddleware, modelCallLimitMiddleware } from 'langchain';
import { createDeepAgent, type SubAgent } from 'deepagents';

type Model = Awaited<ReturnType<typeof initChatModel>>;
type Agent = ReturnType<typeof createDeepAgent>;

interface Env {
    AI_GATEWAY_API_KEY: string;
    AI_GATEWAY_BASE_URL: string;
}

// ─── Unified logger with [subagent][timestamp] prefix ───
const logger = {
    log(...args: unknown[]) {
        console.log(`[subagent][${new Date().toISOString()}]`, ...args);
    },
    error(...args: unknown[]) {
        console.error(`[subagent][${new Date().toISOString()}]`, ...args);
    },
};

const SYSTEM_PROMPT = 'You are a coordinator. Always delegate research tasks to your researcher subagent using the task tool. Keep your final response to one sentence.';

let model: Model | null = null;
let agent: Agent | null = null;

const researchSubagent: SubAgent = {
    name: 'research-agent',
    description: 'Researches topics thoroughly',
    systemPrompt: 'You are a thorough researcher. Research the given topic and provide a concise summary in 2-3 sentences.',
    middleware: [
        modelRetryMiddleware({ maxRetries: 3 }),
        modelCallLimitMiddleware({ runLimit: 30 }),
    ],
};

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
        model = await initChatModel('@Pages/glm-5', {
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
        logger.log('Initializing agent with subagents...');
        agent = createDeepAgent({
            model: modelInstance,
            systemPrompt: SYSTEM_PROMPT,
            subagents: [researchSubagent],
            middleware: [
                modelRetryMiddleware({ maxRetries: 3 }),
                modelCallLimitMiddleware({ runLimit: 30 }),
            ],
        });
    }
    return agent;
}

// Yields SSE events from a subagent-enabled agent using a single combined stream.
async function* eventStream(agentInstance: Agent, userMessage: string, signal?: AbortSignal): AsyncGenerator<string> {
    const input = { messages: [{ role: "user", content: userMessage }] };
    const activeSubagents = new Map<string, { type: string; description: string; status: string }>();
    // Maps tools:UUID namespace → readable agent name (e.g. "research-agent")
    const nsToAgentName = new Map<string, string>();
    // Maps tools:UUID namespace → tool_call_id (namespace UUID is a Pregel Task ID, NOT tool_call_id)
    const nsToToolCallId = new Map<string, string>();
    let currentSource = '';

    try {
        // Single stream with multiple modes: "updates" for lifecycle, "messages" for tokens
        logger.log(`starting combined stream for message: "${userMessage.slice(0, 80)}"`);
        const stream = await agentInstance.stream(input, {
            streamMode: ["updates", "messages"],
            subgraphs: true,
            signal,
        });

        let lastTickAt = Date.now();
        const GAP_THRESHOLD_MS = 3000;

        for await (const [namespace, mode, data] of stream) {
            const gap = Date.now() - lastTickAt;
            if (gap > GAP_THRESHOLD_MS) {
                const ns = Array.isArray(namespace) && namespace.length ? namespace.join('/') : 'main';
                logger.log(`[gap] ${gap}ms before next chunk (ns=${ns}, mode=${mode})`);
            }
            lastTickAt = Date.now();

            if (signal?.aborted) break;
            const isSubagent = namespace.some((s: string) => s.startsWith('tools:'));

            if (mode === 'updates') {
                // ─── Lifecycle tracking (updates mode) ───
                for (const [nodeName, nodeData] of Object.entries(data)) {
                    const messages = (nodeData as any).messages ?? [];

                    // Phase 1: main agent's model_request contains task tool_calls → subagent spawned
                    if (namespace.length === 0 && nodeName === 'model_request') {
                        for (const msg of messages) {
                            for (const tc of msg.tool_calls ?? []) {
                                if (tc.name === 'task') {
                                    activeSubagents.set(tc.id, {
                                        type: tc.args?.subagent_type ?? 'unknown',
                                        description: tc.args?.description?.slice(0, 80) ?? '',
                                        status: 'pending',
                                    });
                                    logger.log(`[lifecycle] PENDING  → subagent "${tc.args?.subagent_type}" (${tc.id})`);
                                    logger.log(`  description: ${tc.args?.description?.slice(0, 80) ?? 'N/A'}`);
                                    yield `data: ${JSON.stringify({ type: 'subagent_lifecycle', status: 'pending', agent: tc.args?.subagent_type, id: tc.id, description: tc.args?.description?.slice(0, 80) })}\n\n`;
                                }
                            }
                        }
                        // Log non-task model_request steps
                        if (!messages.some((m: any) => m.tool_calls?.some((tc: any) => tc.name === 'task'))) {
                            logger.log(`[updates] [main agent] step: ${nodeName}`);
                        }
                    }

                    // Phase 2: events from tools:UUID namespace → subagent is running
                    if (namespace.length > 0 && namespace[0].startsWith('tools:')) {
                        const subagentNs = namespace[0];
                        logger.log(`[updates] [${subagentNs}] step: ${nodeName}`);

                        // Match namespace to a pending subagent (only once per namespace).
                        // Note: the UUID in "tools:UUID" is a Pregel Task ID (uuid5 hash),
                        // which differs from tool_call_id (LLM-generated). We use arrival
                        // order to pair namespaces with pending subagents.
                        if (!nsToToolCallId.has(subagentNs)) {
                            for (const [toolCallId, sub] of activeSubagents) {
                                if (sub.status === 'pending') {
                                    sub.status = 'running';
                                    nsToAgentName.set(subagentNs, sub.type);
                                    nsToToolCallId.set(subagentNs, toolCallId);
                                    logger.log(`[lifecycle] RUNNING  → subagent "${sub.type}" (tool_call_id: ${toolCallId}, ns: ${subagentNs})`);
                                    yield `data: ${JSON.stringify({ type: 'subagent_lifecycle', status: 'running', agent: sub.type, id: toolCallId, namespace: subagentNs })}\n\n`;
                                    break;
                                }
                            }
                        }
                    }

                    // Phase 3: main agent's tools node returns tool messages → subagent finished
                    if (namespace.length === 0 && nodeName === 'tools') {
                        for (const msg of messages) {
                            if (msg.type === 'tool') {
                                const sub = activeSubagents.get(msg.tool_call_id);
                                if (sub) {
                                    sub.status = 'complete';
                                    logger.log(`[lifecycle] COMPLETE → subagent "${sub.type}" (${msg.tool_call_id})`);
                                    logger.log(`  Result preview: ${String(msg.content).slice(0, 200)}...`);
                                }
                                yield `data: ${JSON.stringify({ type: 'subagent_lifecycle', status: 'complete', agent: sub?.type ?? msg.name, id: msg.tool_call_id, content: String(msg.content).slice(0, 500) })}\n\n`;
                            }
                        }
                    }

                    // Log other main agent steps (not model_request or tools)
                    if (namespace.length === 0 && nodeName !== 'model_request' && nodeName !== 'tools') {
                        logger.log(`[updates] [main agent] step: ${nodeName}`);
                    }
                }
            } else if (mode === 'messages') {
                // ─── Token-level streaming (messages mode) ───
                const [message] = data;

                if (isSubagent) {
                    // Token from a subagent
                    const subagentNs = namespace.find((s: string) => s.startsWith('tools:'))!;
                    const agentName = nsToAgentName.get(subagentNs) ?? 'unknown';
                    if (subagentNs !== currentSource) {
                        currentSource = subagentNs;
                        logger.log(`--- [${agentName} (${subagentNs})] ---`);
                        yield `data: ${JSON.stringify({ type: 'source_switch', agent: agentName, namespace: subagentNs })}\n\n`;
                    }
                    if (message.text) {
                        logger.log(`[token] [${agentName}] ${message.text}`);
                        yield `data: ${JSON.stringify({ type: 'ai_response', content: message.text, agent: agentName, namespace: subagentNs })}\n\n`;
                    }
                } else {
                    // Token from the main agent
                    if ('main' !== currentSource) {
                        currentSource = 'main';
                        logger.log('--- [main agent] ---');
                        yield `data: ${JSON.stringify({ type: 'source_switch', agent: 'main' })}\n\n`;
                    }
                    if (message.text) {
                        logger.log(`[token] [main] ${message.text}`);
                        yield `data: ${JSON.stringify({ type: 'ai_response', content: message.text, agent: 'main' })}\n\n`;
                    }
                }
            }
        }
        logger.log('stream completed');
    } catch (e: unknown) {
        const error = e as Error;
        if (error.name === 'AbortError' || signal?.aborted) {
            logger.log('aborted by user');
        } else {
            logger.error('stream error:', error.message, error.stack);
            yield `data: ${JSON.stringify({ type: 'error_message', content: `Stream error: ${error.message}` })}\n\n`;
        }
    }

    // Log final subagent states
    if (activeSubagents.size > 0) {
        logger.log('--- Final subagent states ---');
        for (const [id, sub] of activeSubagents) {
            logger.log(`  ${sub.type}: ${sub.status}`);
        }
    }

    yield "data: [DONE]\n\n";
}

export async function onRequest(context: any) {
    const { request, env } = context;

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
    const readableStream = new ReadableStream({
        async start(controller) {
            const HEARTBEAT_INTERVAL_MS = 5_000;

            // Heartbeat: emit an SSE comment every 5s to keep intermediaries and clients
            // from closing an idle connection.
            const heartbeat = setInterval(() => {
                try {
                    controller.enqueue(encoder.encode(`: ping ${Date.now()}\n\n`));
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

    return new Response(readableStream, {
        status: 200,
        headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
        },
    });
}

// Abort a specific active agent run by conversationId.

// ─── Unified logger with [stop][timestamp] prefix ───
const logger = {
  log(...args: unknown[]) {
    console.log(`[stop][${new Date().toISOString()}]`, ...args);
  },
  error(...args: unknown[]) {
    console.error(`[stop][${new Date().toISOString()}]`, ...args);
  },
};

export async function onRequest(context: any) {
  const { request } = context;
  const conversationId = request?.body?.conversationId as string | undefined;
  logger.log('conversationId:', conversationId);

  if (!conversationId) {
    logger.error('Missing conversationId');
    return new Response('Missing conversationId', { status: 400 });
  }

  const ret = context.utils.abortActiveRun(conversationId);
  logger.log('abortActiveRun result:', ret);

  const data = {
    status: ret?.aborted ? 'aborting' : 'idle',
    conversationId,
    ...ret,
  };

  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'Content-Type': 'application/json; charset=UTF-8' },
  });
}

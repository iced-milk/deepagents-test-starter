// Abort a specific active agent run by conversationId.
export async function onRequest(context: any) {
  const { request } = context;
  const conversationId = request?.body?.conversationId as string | undefined;

  if (!conversationId) {
    return new Response('Missing conversationId', { status: 400 });
  }

  const ret = context.abortActiveRun(conversationId);

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

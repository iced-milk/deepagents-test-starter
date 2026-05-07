export async function onRequest(context: any) {
  const data =  {
    status: 'ok',
    runId: context.run_id,
    env: context.env,
  };

  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'Content-Type': 'application/json; charset=UTF-8' },
  });
}
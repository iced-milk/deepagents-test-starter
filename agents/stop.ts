// 打断当前活跃的 agent 运行
export async function onRequest(context: any) {
  const ret = context.abortActiveRun();
  const data =  {
    status: ret.aborted ? 'aborting' : 'idle',
      ...ret,
  };

  return new Response(JSON.stringify(data), {
    status: 200,
    headers: { 'Content-Type': 'application/json; charset=UTF-8' },
  });
}

# xphi.proxy.gateway
## @lineage: xphi.reflect.gateway
## @lineage: xphi.server.router.gateway
import httpx
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, Response
from watcher.plane.emitter import get_emitter

log = get_emitter("reflect.gateway")
app = FastAPI(title="Brane Reflect Gateway")

TARGET_SERVERS = {
    "legacy": "http://localhost:8000",
    "resource": "http://localhost:8001",
    "tester": "http://localhost:3001",
}

async def verify_token(request: Request):
    auth_header = request.headers.get("Authorization")
    
    ## 예시: 단순 Bearer 토큰 검사
    if not auth_header or not auth_header.startswith("Bearer brane-super-secret-token"):
        log.warning(f"Unauthorized access attempt from {request.client.host}")
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid Brane Token")
    
    ## 토큰이 유효하다면 통과
    return True

@app.api_route("/{target_name}/{path:path}", methods=["GET", "POST", "OPTIONS"])
async def proxy_mcp_requests(target_name: str, path: str, request: Request, _: bool = Depends(verify_token)):
    if target_name not in TARGET_SERVERS:
        raise HTTPException(status_code=404, detail=f"Target server '{target_name}' not found.")

    target_url = f"{TARGET_SERVERS[target_name]}/{path}"
    
    headers = dict(request.headers)
    headers.pop("host", None)

    # [수정됨] async with 블록으로 감싸서 리소스 누수 방지
    async with httpx.AsyncClient() as client:
        try:
            if request.method == "GET":
                # 스트리밍 응답을 위한 제너레이터 
                async def stream_generator():
                    # 스트리밍 요청도 동일한 클라이언트 컨텍스트를 사용
                    async with client.stream("GET", target_url, headers=headers, params=request.query_params) as response:
                        async for chunk in response.aiter_bytes():
                            yield chunk
                
                return StreamingResponse(stream_generator(), media_type="text/event-stream")

            elif request.method == "POST":
                body = await request.body()
                response = await client.post(target_url, content=body, headers=headers)
                return Response(content=response.content, status_code=response.status_code, headers=dict(response.headers))
                
            elif request.method == "OPTIONS":
                response = await client.options(target_url, headers=headers)
                return Response(status_code=response.status_code, headers=dict(response.headers))

        except httpx.RequestError as e:
            log.error(f"Proxy error connecting to {target_name}: {str(e)}")
            raise HTTPException(status_code=502, detail="Bad Gateway: Target MCP server is down.")
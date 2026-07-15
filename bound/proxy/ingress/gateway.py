# bound.proxy.ingress.gateway
## @lineage: anchor.phase.ingress.proxy.gateway
import httpx
import uvicorn
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, Response

from bound.proxy.setting import gateway_settings, pypi_settings
from xphi.xor.secure.auth.token.introspect import IntrospectionTokenVerifier
from watcher.plane.emitter import get_emitter

log = get_emitter("proxy.gateway")
app = FastAPI(title="Brane Reflect Gateway")

TARGET_SERVERS = {
    "pypi": f"http://{pypi_settings.host}:{pypi_settings.port}",
    "legacy": "http://localhost:8000",
    "resource": "http://localhost:8001",
    "tester": "http://localhost:3001",
}

token_verifier = IntrospectionTokenVerifier(
    introspection_endpoint="http://localhost:9000/introspect",
    server_url=f"http://{gateway_settings.host}:{gateway_settings.port}",
    validate_resource=True  # 토큰의 'aud' 클레임이 게이트웨이 URL과 일치하는지 검사
)

async def verify_token(request: Request):
    target_name = request.path_params.get("target_name")
    
    # PyPI 라우팅은 게이트웨이 인증을 우회 (pypi.server 내부 Gatekeeper에 위임)
    if target_name == "pypi":
        return True

    auth_header = request.headers.get("Authorization")
    
    if not auth_header or not auth_header.startswith("Bearer "):
        log.warning(f"Missing or invalid Bearer token format from {request.client.host} to {target_name}")
        raise HTTPException(status_code=401, detail="Unauthorized: Bearer token required")
    
    # "Bearer <token>" 구조에서 실제 토큰 값만 추출
    token_str = auth_header.split(" ")[1]
    
    # 브로커를 호출하여 토큰의 유효성 검증
    verified_token = await token_verifier.verify_token(token_str)
    
    if not verified_token:
        log.warning(f"Token validation failed at Broker for {request.client.host}")
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid or expired Token")
    
    log.info(f"Token verified successfully. Subject: {verified_token.subject}")
    
    # 하위 라우터에서 접근할 수 있도록 상태 객체에 인증된 사용자/주체 정보 저장
    request.state.user = verified_token.subject
    
    return True

@app.api_route("/{target_name}/{path:path}", methods=["GET", "POST", "OPTIONS"])
async def proxy_mcp_requests(target_name: str, path: str, request: Request, _: bool = Depends(verify_token)):
    if target_name not in TARGET_SERVERS:
        raise HTTPException(status_code=404, detail=f"Target server '{target_name}' not found.")

    target_url = f"{TARGET_SERVERS[target_name]}/{path}"
    headers = dict(request.headers)
    headers.pop("host", None)

    async with httpx.AsyncClient() as client:
        try:
            if request.method == "GET":
                async def stream_generator():
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
            raise HTTPException(status_code=502, detail=f"Bad Gateway: Target {target_name} is down.")
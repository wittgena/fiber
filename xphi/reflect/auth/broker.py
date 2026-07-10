# xphi.reflect.auth.broker
## @lineage: xphi.proxy.auth.broker
"""
@desc: 
- Central Authorization Server (Broker) for the Brane Infrastructure.
- Issues tokens and provides RFC 7662 compliant Introspection.
"""
import time
from fastapi import FastAPI, Depends, HTTPException, status, Form, Request
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import Dict, Any

from xphi.reflect.auth.schemas import TokenResponse
from watcher.plane.emitter import get_emitter
from anchor.registry.proxy.setting import default_auth_settings, gateway_settings

log = get_emitter("auth.broker")
app = FastAPI(title="Brane Auth Broker", version="1.0")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@app.post("/token", response_model=TokenResponse)
async def login_for_access_token(
    grant_type: str = Form(...),
    username: str = Form(None),
    password: str = Form(None),
    client_id: str = Form(None),
    client_secret: str = Form(None)
):
    if grant_type == "client_credentials":
        ## 자동화 에이전트(Builder 등) 인증
        if client_id != "brane-internal-client" or client_secret != "super-secret":
            log.warning(f"Failed client_credentials attempt for client: {client_id}")
            raise HTTPException(status_code=401, detail="Invalid client credentials")
        log.info(f"Issuing access token for automated client: {client_id}")
    elif grant_type == "password":
        ## 일반 사용자(UI) 인증
        if username != default_auth_settings.demo_username or password != default_auth_settings.demo_password:
            log.warning(f"Failed password login attempt for user: {username}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        log.info(f"Issuing access token for user: {username}")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported grant_type: {grant_type}")
    
    return TokenResponse(
        access_token=gateway_settings.secret_token,
        token_type="bearer",
        expires_in=3600 # 1 hour
    )

@app.post("/introspect")
async def introspect_token(request: Request, token: str = Form(...)) -> Dict[str, Any]:
    """@desc: RFC 7662 compliant Token Introspection endpoint."""
    is_valid = (token == gateway_settings.secret_token)
    
    if is_valid:
        log.info("Token introspection successful (Active).")
        expected_gateway_url = f"http://{gateway_settings.host}:{gateway_settings.port}"
        return {
            "active": True,
            "scope": default_auth_settings.mcp_scope,
            "client_id": "brane-internal-client",
            "username": default_auth_settings.demo_username,
            "exp": int(time.time()) + 3600, 
            "aud": expected_gateway_url, # Gateway의 리소스 URL 검증용
            "sub": "brane-admin-subject"
        }
    else:
        log.warning("Token introspection failed (Inactive/Invalid).")
        return {"active": False}

@app.get("/.well-known/openid-configuration")
async def discovery_endpoint(request: Request):
    base_url = str(request.base_url).rstrip("/")
    return {
        "issuer": base_url,
        "authorization_endpoint": f"{base_url}/authorize",
        "token_endpoint": f"{base_url}/token",
        "introspection_endpoint": f"{base_url}/introspect",
        "response_types_supported": ["code", "token"],
        "subject_types_supported": ["public"],
    }
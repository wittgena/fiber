# bound.reflect.server.auth.schemas
## @lineage: xphi.reflect.server.auth.schemas
## @lineage: xphi.reflect.auth.schemas
## @lineage: xphi.proxy.auth.schemas
from pydantic import BaseModel, Field

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int

class IntrospectRequest(BaseModel):
    token: str

class IntrospectResponse(BaseModel):
    active: bool
    scope: str | None = None
    client_id: str | None = None
    username: str | None = None
    exp: int | None = None
    aud: str | None = None
    sub: str | None = None

class AuthConstants:
    BROKER_TOKEN_ENDPOINT = "/token"
    BROKER_INTROSPECT_ENDPOINT = "/introspect"
    INTERNAL_CLIENT_ID = "brane-internal-client"
    INTERNAL_CLIENT_SECRET = "super-secret"
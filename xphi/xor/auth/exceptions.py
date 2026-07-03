# xphi.xor.auth.exceptions
## @lineage: xphi.auth.exceptions
## @lineage: bound.auth.exceptions
## @lineage: bound.bridge.auth.exceptions
## @lineage: xphi.server.auth.xphi.exceptions
class OAuthFlowError(Exception):
    """Base exception for OAuth flow errors."""

class OAuthTokenError(OAuthFlowError):
    """Raised when token operations fail."""

class OAuthRegistrationError(OAuthFlowError):
    """Raised when client registration fails."""

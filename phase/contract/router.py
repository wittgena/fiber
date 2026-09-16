# fiber.phase.contract.router
## @lineage: xphi.arch.contract.protocol.router
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Protocol, Callable, Tuple
from fastapi import APIRouter

class ContractRouter(APIRouter):
    def __init__(self, namespace: str, *args: Any, **kwargs: Any):
        self.description = kwargs.pop("description", None)
        self.summary = kwargs.pop("summary", None)
        
        super().__init__(*args, **kwargs)
        self.namespace = namespace

    def add_api_route(self, path: str, endpoint: Callable[..., Any], **kwargs: Any) -> None:
        if "name" not in kwargs:
            kwargs["name"] = f"{self.namespace}.{endpoint.__name__}"
            
        super().add_api_route(path, endpoint, **kwargs)
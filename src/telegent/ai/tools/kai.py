"""HTTP boundary for the shared Kai backend; task work runs on the backend."""
from __future__ import annotations

from typing import Any

import httpx

from telegent.ai.tools.spec import ToolContext, ToolSpec


class KaiTools:
    def __init__(self, base: str, token: str, *, transport: httpx.AsyncBaseTransport | None = None):
        self.base = base.rstrip("/")
        self.token = token
        self.transport = transport

    def specs(self) -> list[ToolSpec]:
        definitions = {
            "get_status": ("Read current Kai task status.", {}),
            "create_task": ("Queue work asynchronously; acknowledge on it, never claim completion.", {"title": {"type": "string"}}),
            "capture_note": ("Capture a note.", {"text": {"type": "string"}}),
            "request_confirm": ("Request human confirmation; this does not grant approval.", {"question": {"type": "string"}}),
        }
        result = []
        for name, (description, properties) in definitions.items():
            def handler_for(tool_name):
                async def handler(ctx: ToolContext, args: dict[str, Any]) -> dict[str, Any]:
                    async with httpx.AsyncClient(transport=self.transport, timeout=4, follow_redirects=False) as client:
                        options: dict[str, Any] = (
                            {"params": {"user_id": ctx.user_id, "call_id": ctx.call_id}}
                            if tool_name == "get_status" else
                            {"json": {**args, "user_id": ctx.user_id, "call_id": ctx.call_id}}
                        )
                        response = await client.request(
                            "GET" if tool_name == "get_status" else "POST",
                            f"{self.base}/{tool_name}",
                            headers={"Authorization": f"Bearer {self.token}"},
                            **options,
                        )
                        if not response.is_success:
                            raise RuntimeError(f"Kai API returned HTTP {response.status_code}")
                        data = response.json()
                        if tool_name == "create_task":
                            if response.status_code != 202:
                                raise RuntimeError("Kai task endpoint must return 202 Accepted")
                            return {"message": "on it", "task": data}
                        return data
                return handler
            result.append(ToolSpec(
                name=name, description=description, handler=handler_for(name),
                parameters_json_schema={"type": "object", "properties": properties,
                                        "required": list(properties)},
            ))
        return result

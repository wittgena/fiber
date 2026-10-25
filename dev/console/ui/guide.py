# fiber.dev.console.ui.guide
from __future__ import annotations

from nicegui import ui

from fiber.infra.ops.account.state import refs
from fiber.dev.console.ui.kit import ModalTemplate

LLM_GATEWAY_YAML = """models:
  # apiKey: Omitted. Daemon resolves keys from host env (.env / OS).
  - name: Command R (Cohere)
    provider: openai
    model: cohere/command-r-08-2024
    apiBase: http://127.0.0.1:8356/v1

  - name: Qwen 3.8 27B (Cerebras)
    provider: openai
    model: cerebras/qwen-3.8-27b
    apiBase: http://127.0.0.1:8356/v1

  - name: Gemma 3 1B (Local Engine)
    provider: openai
    model: llama_server/gemma-3-1b-it-Q4_K_M.gguf
    apiBase: http://127.0.0.1:8356/v1"""

MCP_GATEWAY_JSON = """{
  "mcpServers": {
    "fiber-gateway": {
      "url": "http://127.0.0.1:8356/v1/mcp-gateway/invoke"
    },
    "repomix": {
      "command": "npx",
      "args": ["-y", "repomix", "--mcp"],
      "env": {
        "FIBER_PREFLIGHT_PATTERN": "button:DROP|TRUNCATE|DELETE"
      }
    }
  }
}"""

CONNECT_CLI_SH = """# Auto-Scan & Sync local IDE configs (.mcp.json) to ~/.anchor/mcp.json
$ fiber connect copy

# Lock 3rd-party stdio MCP with 1-Click Slack Button ('button:' prefix)
$ fiber connect copy -t local-fs -e "npx -y @modelcontextprotocol/server-filesystem /tmp" -p "button:prod_secrets|DELETE" """


class GuideModal(ModalTemplate):
    width_cls = "w-[720px] max-w-[95vw]"
    border_cls = "border border-slate-700"
    persistent = False

    def compose(self, dialog: ui.dialog) -> None:
        setattr(refs, "guide_dialog", dialog)

        with ui.row().classes("w-full items-center justify-between border-b border-slate-800 pb-2 mb-1"):
            ui.label("IDE Setup Guide").classes("text-sm font-bold text-white tracking-wide")
            ui.button(icon="close", on_click=dialog.close).props("flat dense round size=sm color=slate-4")

        with ui.tabs().classes("w-full text-xs bg-slate-950 rounded border border-slate-800") as tabs:
            tab_llm = ui.tab("LLM Gateway")
            tab_mcp = ui.tab("MCP Gateway & HITL")

        with ui.tab_panels(tabs, value=tab_llm).classes("w-full bg-transparent p-0 mt-2"):
            with ui.tab_panel(tab_llm).classes("p-0 gap-2 flex flex-col"):
                ui.label(
                    "Route IDE model requests through Fiber's guardrails and FinOps breaker without exposing API keys in client configs."
                ).classes("text-xs text-slate-400")
                ui.label("• Endpoint: http://127.0.0.1:8356/v1  |  Provider: openai").classes(
                    "text-xs font-mono text-emerald-400"
                )
                ui.code(LLM_GATEWAY_YAML, language="yaml").classes("w-full text-xs")

            with ui.tab_panel(tab_mcp).classes("p-0 gap-2 flex flex-col"):
                ui.label(
                    "Inject 'fiber-gateway' into your agent config (.mcp.json, .cursor/mcp.json, .vscode/mcp.json) and attach optional pre-flight regex traps."
                ).classes("text-xs text-slate-400")
                ui.code(MCP_GATEWAY_JSON, language="json").classes("w-full text-xs")
                ui.label("Declarative Sync & Hot-Reload CLI:").classes("text-xs font-bold text-cyan-400 mt-1")
                ui.code(CONNECT_CLI_SH, language="bash").classes("w-full text-xs")

def open_guide_modal() -> None:
    dlg = getattr(refs, "guide_dialog", None)
    if dlg is not None:
        dlg.open()
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

CONNECT_CLI_SH = """# Explicitly scan & sync IDE configs (e.g., 'claude', 'cursor', or 'all') to ~/.anchor/mcp.json
$ fiber connect claude
$ fiber connect all

# Register & lock 3rd-party stdio MCP with 1-Click Slack Button ('button:' prefix)
$ fiber connect -t local-fs -e "npx -y @modelcontextprotocol/server-filesystem /tmp" -p "button:prod_secrets|DELETE"

# Standalone Mode (without daemon): Run a worker directly in the foreground
$ fiber connect --standalone -t prod-db -e "npx -y @modelcontextprotocol/server-postgres" -p "DROP|DELETE" """

GUARDRAIL_RULES_JSON = """{
  "patterns": {
    "fiber_internal_token": "FIBER-INTERNAL-TOKEN-[0-9]{6}",
    "corp_vault_key": "CORP-VAULT-[A-Z0-9]{16}",
    "slack_token": "xox[baprs]-[A-Za-z0-9_-]{24,}"
  },
  "disabled_rules": [
    "context7_key",
    "generic_password_kv"
  ],
  "extra_secret_keys": [
    "FIBER_VAULT_PIN",
    "INTERNAL_AUTH_HEADER"
  ]
}"""

COMPAT_RULES_JSON = """{
  "provider_param_rules": {
    "my_custom_llm": {
      "message_schema": "openai_array",
      "supported": ["temperature", "max_tokens", "stream"],
      "mapping": {
        "max_tokens": "max_output_tokens"
      }
    }
  },
  "extraction_rules": {
    "ollama": {
      "text": ["choices.0.delta.content", "message.content", "response"],
      "finish_reason": ["done_reason", "choices.0.finish_reason"],
      "is_finished_cond": {"path": "done", "value": true},
      "usage": [
        "usage",
        {"prompt_tokens": "prompt_eval_count", "completion_tokens": "eval_count"}
      ]
    }
  }
}"""


class GuideModal(ModalTemplate):
    width_cls = "w-[760px] max-w-[95vw]"
    border_cls = "border border-slate-700"
    persistent = False

    def compose(self, dialog: ui.dialog) -> None:
        setattr(refs, "guide_dialog", dialog)

        with ui.row().classes("w-full items-center justify-between border-b border-slate-800 pb-2 mb-1"):
            ui.label("IDE & Declarative Registry Guide").classes("text-sm font-bold text-white tracking-wide")
            ui.button(icon="close", on_click=dialog.close).props("flat dense round size=sm color=slate-4")

        with ui.tabs().classes("w-full text-xs bg-slate-950 rounded border border-slate-800") as tabs:
            tab_llm = ui.tab("LLM Gateway")
            tab_mcp = ui.tab("MCP Gateway & HITL")
            tab_guard = ui.tab("Guardrail Rules")
            tab_compat = ui.tab("Compat Rules")

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

            with ui.tab_panel(tab_guard).classes("p-0 gap-2 flex flex-col"):
                ui.label(
                    "33 built-in credential regex rules and 14 sensitive key tokens are active out of the box. To add custom patterns, override rules, or disable specific rule IDs without code changes, supply 'redact_rules.json'."
                ).classes("text-xs text-slate-400")
                ui.label(
                    "• Env Override: FIBER_REDACT_RULES_PATH=/path/to/redact_rules.json  |  Default: .anchor/registry/redact_rules.json"
                ).classes("text-xs font-mono text-emerald-400")
                ui.code(GUARDRAIL_RULES_JSON, language="json").classes("w-full text-xs")

            with ui.tab_panel(tab_compat).classes("p-0 gap-2 flex flex-col"):
                ui.label(
                    "Standard providers (openai, gemini, cohere, cerebras, ollama, llama_server) are normalized out of the box. To onboard custom models or override dot-notation extraction paths declaratively, supply 'compat_rules.json'."
                ).classes("text-xs text-slate-400")
                ui.label(
                    "• Env Override: FIBER_COMPAT_RULES_PATH=/path/to/compat_rules.json  |  Validate: fiber compat fixture --vcr replay"
                ).classes("text-xs font-mono text-cyan-400")
                ui.code(COMPAT_RULES_JSON, language="json").classes("w-full text-xs")

def open_guide_modal() -> None:
    dlg = getattr(refs, "guide_dialog", None)
    if dlg is not None:
        dlg.open()
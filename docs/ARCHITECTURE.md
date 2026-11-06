# Architecture, Protocol Normalization & Cortex AI Gateway Integration

> **Disclaimer:** This project is an independent community / field-engineering implementation built on top of Snowflake Cortex AI Gateway and Snowpark Container Services (SPCS). It is **NOT an official Snowflake product** and is released under the **[MIT License](../LICENSE)**.

## 1. Three Native Inference Surfaces + OTLP Telemetry Pass-Through

`snowflake-proxy-cortex-aigateway` exposes three distinct inference protocol prefixes plus an OTLP telemetry pass-through route on port `8080`, routing validated requests to `https://<account-host>/api/v2/aigateways/<gateway-name>`:

| Surface | Client Base URL | Discovery Route | Inference / Telemetry Route | Upstream Cortex AI Gateway Path |
|---|---|---|---|---|
| **OpenAI Chat Completions** | `http://<proxy>:8080/chat/v1` | `GET /chat/v1/models` | `POST /chat/v1/chat/completions` | `/api/v2/aigateways/<gw>/v1/chat/completions` |
| **OpenAI Responses** | `http://<proxy>:8080/responses/v1` | `GET /responses/v1/models` | `POST /responses/v1/responses` | `/api/v2/aigateways/<gw>/v1/responses` |
| **Anthropic Messages** | `http://<proxy>:8080/messages` (or `/messages/v1`) | `GET /messages/v1/models` | `POST /messages/v1/messages` | `/api/v2/aigateways/<gw>/v1/messages` |
| **OTLP Client Telemetry** | `http://<proxy>:8080` | — | `POST /telemetry/v1/traces` | `/api/v2/aigateways/<gw>/telemetry/v1/traces` |

> **Why `/v1/models` is Served by the Proxy:** Snowflake Cortex AI Gateway does not expose a `GET /v1/models` discovery route (`HTTP 404: Not a supported AI Gateway path`). The proxy resolves `GET /<protocol>/v1/models` locally from its **Single-Writer Model Registry (`:8090`)**, returning only models cryptographically qualified for that specific protocol.

## 2. Authentication Translation (SPCS OAuth) & Host Normalization

Inside Snowpark Container Services, Snowflake mounts a rotated OAuth token at `/snowflake/session/token` and provides `SNOWFLAKE_HOST`.
- The proxy automatically normalizes underscores (`_`) to hyphens (`-`) in `SNOWFLAKE_HOST` for valid TLS wildcard hostname verification on `*.snowflakecomputing.com`.
- Internal consumers (`Hermes`, `Langflow`, `SnowflakeLangflow`, `n8n`, `LangChain`) connect over internal SPCS DNS (`*.svc.spcs.internal:8080`) using a dummy placeholder API key (never forwarded upstream), and the proxy injects on every inference and telemetry request:
  - `Authorization: Bearer <token_from_/snowflake/session/token>`
  - `X-Snowflake-Authorization-Token-Type: OAUTH`
  - `anthropic-version: 2023-06-01` (on `/messages/v1/messages`)

## 3. W3C Trace Context & Gateway Observability Header Propagation

To enable unified client + server trace correlation in `TABLE(AGENT_TRACE_TABLE('<GATEWAY>'))`, `app.py` (`_gateway_observability_headers`) validates and forwards four optional observability headers while stripping all untrusted client headers (`x-api-key`, `x-user`, etc.):

1. **`traceparent`** (`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`): W3C Trace Context ID linking client spans and Cortex AI Gateway server spans under a single `trace_id`.
2. **`tracestate`** (`^[\x20-\x7e]{1,512}$`): W3C vendor trace state.
3. **`x-snowflake-ai-gateway-conversation-id`** (`^[A-Za-z0-9_.:@-]{1,128}$`): Session/conversation correlation ID stored in `RECORD_ATTRIBUTES:"gen_ai.conversation.id"` (also mapped automatically if the client sends `x-claude-code-session-id` or `x-conversation-id`).
4. **`snow-agent-name`** (`^[A-Za-z0-9_.:@-]{1,64}$`): Agent identifier stored in `RECORD_ATTRIBUTES:"http.request.header.snow-agent-name"`.

## 4. Universal Client Normalization (`rules.py`)

When `ProfileV2.normalize_client_options = True`, the proxy transparently normalizes incoming client payloads (and records all adjustments in the `x-proxy-adjustments` response header):

1. **Token Limits (`1..128000`):** Accepts any positive integer for `max_tokens`, `max_completion_tokens`, and `max_output_tokens`, clamping values above the model's upper bound. On `messages`, defaults `max_tokens = 4096` if omitted. On `responses`, leaves `max_output_tokens` unset when omitted by the client so long reasoning blocks do not truncate downstream `function_call` JSON arguments.
2. **Responses Protocol Adjustments:**
   - Forces `store = false` (required by stateless Cortex AI Gateway Responses, even when n8n sends `store = true`).
   - Adds `include = ["reasoning.encrypted_content"]` when `reasoning` capability is enabled.
   - Strips empty `"text": {}` objects sent by n8n that would otherwise trigger `HTTP 400` upstream.
   - Strips unsupported `temperature` / `top_p` fields and `prompt_cache_retention` hints on reasoning Responses models.
   - Clamps out-of-domain `reasoning.effort` values (e.g. `"none"` / `"minimal"` -> `"low"`) and `reasoning.summary` values to the profile's declared enum domain.
   - Enables `response_stream = "ordered-items"` (`stream_order.py` + `protocol.py`), reordering interleaved SSE events, accepting empty or provisional `encrypted_content` on `response.output_item.added`, verifying final `encrypted_content` equality between `response.output_item.done` and `response.completed`, and backfilling `response.output_item.added` so `langchain-openai`, `Langflow`, `Hermes`, and `n8n` receive the finalized opaque payload cleanly.
3. **Chat Protocol Adjustments:**
   - Converts `max_tokens` &rarr; `max_completion_tokens` when `max_completion_tokens` is absent (`rename-max-tokens-to-completion`), and resolves conflicts when a client sends both (`dedup-max-tokens`).
   - Sets `reasoning_effort = "none"` when required by a profile (even if the client sends `"medium"` or omits it), and removes `reasoning_effort` on Chat models that do not support it.
4. **Messages Protocol Adjustments:**
   - When `thinking.type` is outside the profile's supported enum (for example `"adaptive"` sent by clients before profile negotiation), strips the unsupported `thinking` block and paired `output_config.effort` instead of failing with `HTTP 400`.
   - When `thinking.type == "enabled"`, strips incompatible `temperature` / `top_p` parameters and bumps `max_tokens` if `max_tokens <= thinking.budget_tokens`.
   - Normalizes string `tool_choice` (`"auto"` / `"none"`) to `{"type": ...}`.
5. **Multi-Round Tool Call ID Deduplication:**
   - Automatically renames reused tool call IDs (`call_0..call_N`) from prior closed turns (`call_0` -> `call_0_r2`) in both the assistant turn and matching tool result.
6. **1M+ Token Context & Long-Stream Resource Bounds (`app.py`):**
   - Defaults `max_body = 32 MiB` (`PROXY_MAX_BODY=33554432`), `max_response = 32 MiB` (`PROXY_MAX_RESPONSE=33554432`), `deadline = 600s` (`PROXY_DEADLINE=600`), and upstream `httpx.Timeout(180, connect=10, write=60, pool=5)` so 1M+ token context windows (`openai-gpt-6-astra`, `openai-gpt-6.1-sol`, `claude-opus-5-5`) with multi-round `reasoning.encrypted_content` blobs and long context-compression streams complete without premature `HTTP 413` rejections or mid-stream cutoff.

## 5. Stateless Client `ReplayCache` (`replay_cache.py`)

A bounded in-memory LRU cache (`max_entries=512`, `ttl=3600s`) inspects outgoing JSON and SSE responses for `responses` and `messages`:
- When a response emits tool calls (`function_call` or `tool_use`) alongside `reasoning.encrypted_content` (Responses) or `thinking` / `signature` / `redacted_thinking` blocks (Messages), it caches those blocks keyed by tool call ID.
- If the client's follow-up request omits those blocks (for example, unpatched n8n Agent engine serialization), `ReplayCache.rehydrate()` restores them automatically before forwarding to Cortex AI Gateway.

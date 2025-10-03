import os
import json
import uuid
import logging
from typing import Any

from .graph import AGENT
from .storage import get_redis_client

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
log = logging.getLogger(__name__)
log.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# -----------------------------------------------------------------------------
# Config / constants
# -----------------------------------------------------------------------------
STREAM_FIELD = "chunkOutput"
START_MARKER = "START;<br/>"
END_MARKER = "<br/>;END"

STREAM_MAXLEN = int(os.getenv("REDIS_STREAM_MAXLEN", "5000"))   # approx cap
STREAM_TTL_SEC = int(os.getenv("REDIS_STREAM_TTL_SEC", "600"))  # 10 minutes
STREAM_INCLUDE_FINAL_JSON = os.getenv("STREAM_INCLUDE_FINAL_JSON", "false").lower() == "true"


def _jsonify_rows(val: Any):
    """If val is a JSON string, parse it; otherwise return as-is."""
    try:
        if isinstance(val, str):
            return json.loads(val)
    except Exception:
        pass
    return val


def _as_stream_value(v: Any) -> str:
    """XADD only accepts bytes/str/int/float. Encode dict/list to JSON."""
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str, ensure_ascii=False)
    if isinstance(v, (str, bytes, int, float)):
        return v  # type: ignore[return-value]
    return str(v)


def _xadd(rc, key: str, value: Any):
    """Safe XADD with MAXLEN ~ STREAM_MAXLEN."""
    payload = _as_stream_value(value)
    return rc.xadd(
        key,
        {STREAM_FIELD: payload},
        maxlen=STREAM_MAXLEN,
        approximate=True,
    )


def handler(event, context):
    # API Gateway can send event as string sometimes
    if isinstance(event, str):
        try:
            event = json.loads(event)
        except Exception:
            event = {}

    event = event or {}
    log.info("TELme_ION_Agent invoked: keys=%s", list(event.keys()))

    # Extract query/question (support both keys)
    q = event.get("question") or event.get("query")
    if not q:
        return {"statusCode": 400, "body": "Missing 'question' (or 'query') in event"}

    # Metadata (optional but recommended)
    user_id = event.get("user_id")
    roles = event.get("roles")
    session_id = event.get("session_id") or str(uuid.uuid4())

    # Stream key (Redis Cluster hashtag to keep related keys in one slot)
    raw_key = event.get("key") or session_id
    stream_key = f"query:{raw_key}"

    # Connect Redis (non-fatal if it fails; we still compute a response)
    redis_client = None
    try:
        redis_client = get_redis_client()
        log.info("[Main] Redis stream started for key=%s", stream_key)
        _xadd(redis_client, stream_key, START_MARKER)
        redis_client.expire(stream_key, STREAM_TTL_SEC)
    except Exception as re:
        log.warning("Redis prime failed: %s", re)

    # Publisher used by graph to emit streaming chunks
    def publish(payload: Any):
        """
        Send stream chunks to Redis.

        Behavior:
        - {'token': '...'}  -> push the raw string token (no JSON wrapper).
        - {'note': '...'}   -> ignored (debug/diagnostics).
        - any other dict    -> IGNORED by default to avoid JSON being shown
                              (set STREAM_INCLUDE_FINAL_JSON=true to allow).
        """
        if not redis_client:
            return
        try:
            if isinstance(payload, dict):
                if set(payload.keys()) == {"token"}:
                    _xadd(redis_client, stream_key, payload["token"])
                elif "note" in payload:
                    # drop diagnostics from the visible stream
                    return
                elif STREAM_INCLUDE_FINAL_JSON:
                    _xadd(redis_client, stream_key, payload)
                else:
                    # ignore non-token dicts to avoid duplicate JSON showing in FE
                    return
            else:
                _xadd(redis_client, stream_key, payload)
        except Exception as se:
            log.warning("Redis stream publish failed: %s", se)

    try:
        # Invoke the agent graph (it may stream by calling publish({...}))
        state = AGENT.invoke({
            "question": q,
            "session_id": session_id,
            "user_id": user_id,
            "roles": roles,
            "publish": publish
        })

        rows = _jsonify_rows(state.get("rows", "[]"))
        body = {
            "sql": state.get("sql"),
            "rows": rows,
            "insights": state.get("answer"),
            "session_id": session_id
        }

        # IMPORTANT:
        # Do not stream the final JSON by default to avoid duplicate output in FE.
        if redis_client:
            try:
                if STREAM_INCLUDE_FINAL_JSON:
                    _xadd(redis_client, stream_key, body)
                _xadd(redis_client, stream_key, END_MARKER)
            except Exception as se:
                log.warning("Redis stream publish failed: %s", se)

        # Return structured JSON in HTTP response
        return {
            "statusCode": 200,
            "body": json.dumps(body, default=str, ensure_ascii=False)
        }

    except Exception as e:
        log.exception("Unhandled error")
        err_body = {"error": str(e), "session_id": session_id}

        if redis_client:
            try:
                _xadd(redis_client, stream_key, err_body if STREAM_INCLUDE_FINAL_JSON else "[ERROR]")
                _xadd(redis_client, stream_key, END_MARKER)
            except Exception as se:
                log.warning("Redis error publish failed: %s", se)

        return {
            "statusCode": 500,
            "body": json.dumps(err_body, default=str, ensure_ascii=False)
        }

import os, json, time
from functools import lru_cache
import redis.asyncio as aredis
import datetime

REDIS_HOST = os.getenv(
    "REDIS_HOST", "aws-te-tel-me-redis-dev-t1pfjd.serverless.use1.cache.amazonaws.com"
)


@lru_cache()
def get_redis_client():
    return aredis.Redis(
        host=REDIS_HOST,
        ssl=True,
        ssl_cert_reqs="none",
        decode_responses=True,
        retry_on_timeout=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
    )


async def update_agentic_status(redis_client, key_id, status):
    """
    `status` must in be the format of:
    STATUS-YOUR_MESSAGE
    COMPLETE-YOUR_MESSAGE
    ERROR-YOUR_MESSAGE
    """
    time.sleep(2)
    try:
        result = await redis_client.publish(key_id, f"{status}")
        print(
            f"{status} {key_id} message published successfully. {result} clients received the message."
        )
    except Exception as e:
        print(f"Error publishing message: {e}")


async def get_next_chat_id(redis_client, session_id):
    """
    Retrieves the next chat ID for a given session.

    Args:
    - redis_client (redis.RedisCluster): The Redis client instance.
    - session_id (str): The unique ID for the session (UUID).

    Returns:
    - (int): The next chat ID to use.
    """
    chat_id_key = f"session:{session_id}:next_chat_id"

    # Increment the chat ID in Redis and return the new value
    try:
        return await redis_client.incr(chat_id_key)
    except Exception as e:
        print(f"[Storage] Error incrementing chat ID for session {session_id}: {e}")
        return None


async def get_user_id_for_session(redis_client, session_id):
    """
    Retrieves the user ID associated with a given session ID.

    Args:
    - redis_client (redis.RedisCluster): The Redis client instance.
    - session_id (str): The unique ID of the session.

    Returns:
    - (str): The user ID associated with the session, or an emtpy string if not found
    """
    session_info_key = f"session:{session_id}:info"
    try:
        user_id_stored = await redis_client.hget(session_info_key, "user_id")
        return user_id_stored if user_id_stored else ""
    except Exception as e:
        print(f"[Storage] Error retrieving user ID for session {session_id}: {e}")
        return ""


async def get_final_output(redis_client, stream_key):
    """
    Retrieve final output from redis stream
    """
    res = ""
    messages = await redis_client.xrange(stream_key, "-", "+")
    for m in messages[1:-1]:
        res += m[1].get("chunkOutput", "")

    return res


async def store_chat_data(
    redis_client,
    session_id,
    user_id="",
    roles="",
    query="",
    answer="",
    sources=[],
    class_label="",
):
    """
    Stores chat data into Redis with a new session ID or an existing session ID, and checks user ID validity.

    Args:
    - redis_client (redis.RedisCluster): The Redis client instance.
    - session_id (str): The unique ID for the session (UUID).
    - user_id (str, optional): The unique ID of the user.
    - roles (str, optional): The user roles, e.g., json.dumps(["Class3Viewer"]).
    - query (str, optional): The query from the user.
    - answer (str, optional): The answer to the user query.
    - sources (list, optional): A list of sources related to the chat answer.
    - class_label (str, optional): The classified query label.

    Returns:
    - (dict): A dictionary with the status and message of the operation.

    Raises:
    - (ValueError): If the provided user ID does not match the user ID associated with the session.
    """

    try:
        user_sessions_key = f"user:{user_id}:sessions:sorted"
        session_info_key = f"session:{session_id}:info"
        current_time = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
        current_timestamp_str = current_time.isoformat()
        current_timestamp_float = current_time.timestamp()

        if not await redis_client.zscore(user_sessions_key, session_id):
            await redis_client.zadd(
                user_sessions_key,
                {session_id: current_timestamp_float + 7 * 24 * 60 * 60},
            )
            await redis_client.hset(
                session_info_key,
                mapping={
                    "title": query,
                    "latest_timestamp": current_timestamp_str,
                    "user_id": user_id,
                    "roles": roles,
                },
            )
            await redis_client.expire(session_info_key, 7 * 24 * 60 * 60)  # 7 days TTL
        else:
            # Validate the user ID associated with the existing session ID
            session_user_id = await get_user_id_for_session(redis_client, session_id)
            if session_user_id != user_id:
                raise ValueError("Improper user ID for the provided session ID.")

        chat_id = await get_next_chat_id(redis_client, session_id)
        chat_key = f"session:{session_id}:chat:{chat_id}"
        chat_data = {
            "timestamp": current_timestamp_str,
            "query": query,
            "response": answer,
            "sources": json.dumps(sources),  # convert list to JSON string
            "class_label": class_label,
            "user_id": user_id,
            "roles": roles,
        }
        await redis_client.hset(chat_key, mapping=chat_data)
        await redis_client.expire(chat_key, 7 * 24 * 60 * 60)  # 7 days TTL

        # Track chat ID in a list
        await redis_client.rpush(f"session:{session_id}:chat_id", chat_id)
        # Trim the sorted set to keep only the latest 20 sessions & 7 days TTL
        await redis_client.zremrangebyrank(user_sessions_key, 0, -21)
        await redis_client.zremrangebyscore(
            user_sessions_key, "-inf", current_timestamp_float
        )
    except Exception as e:
        # Log the exception
        print(f"[Storage] Error storing chat data: {e}")
        return {"status": "Failure", "message": str(e), "session_id": session_id}
    return {
        "status": "Success",
        "message": "Data stored successfully.",
        "session_id": session_id,
    }

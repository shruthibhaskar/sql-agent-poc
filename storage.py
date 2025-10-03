import re
import os
import json
import datetime
from redis import RedisCluster, StrictRedis
import redis.asyncio as aredis
#from .logger import log_event


def extract_score(text):
    """
    Extract the score from a given text.

    Args:
    text (str): The input text containing the score.

    Returns:
    int or None: The extracted score as an integer, or None if not found.
    """
    pattern = r"your final score is:\s*(\d+)"
    match = re.search(pattern, text, re.IGNORECASE)
    return int(match.group(1)) if match else None


def aget_redis_client(decode_responses=True):
    try:
        client = aredis.Redis(
            host=os.getenv("REDIS_HOST"),
            ssl=True,
            ssl_cert_reqs="none",
            decode_responses=decode_responses,
        )
        print("[Storage] Connected to Redis successfully!")
        return client
    except Exception as e:
        print(f"[Storage] Error connecting to Redis: {e}")


def get_redis_client(decode_responses=True):
    """
    Create and return a redis.RedisCluster client instance.

    Returns:
    - (redis.RedisCluster): A RedisCluster client instance if the connection is successful.

    Raises:
    - e (Exception): If there is an error connecting to the Redis cluster, it prints an error message.
    """
    try:
        client = RedisCluster(
            host=os.getenv("REDIS_HOST"),
            # port=6379,
            ssl=True,
            ssl_cert_reqs="none",
            decode_responses=decode_responses,
        )
        print("[Storage] Connected to Redis successfully!")
        return client
    except Exception as e:
        print(f"[Storage] Error connecting to Redis: {e}")


def data_redirect(e):
    """
    Handle Redis MOVED error while storing data.
    This function manages the MOVED error by redirecting the Redis client to the new node specified in the error message.

    Args:
    - e (Exception): The exception object containing the MOVED error details.

    Returns:
    - (redis.StrictRedis): A new Redis client connected to the redirected node if successful.
    - (Exception): The exception object if the connection to the new node fails.

    Raises:
    - e (Exception): If the error is not a MOVED error, it re-raises the original exception.
    """
    if e.args[0].startswith("MOVED"):
        # Extract the new node information from the error response
        _, _, new_node = e.args[0].split(" ")
        new_host, new_port = new_node.split(":")
        # Update the Redis client connection to the new node
        redis_client = StrictRedis(
            host=new_host, port=int(new_port), ssl=True, decode_responses=True
        )
        try:
            redis_client.ping()
            return redis_client
        except Exception as e:
            return e
    else:
        # Handle other types of Redis errors
        raise e


def store_metadata(
    redis_client, stream_key, query, class_label, user_id, roles, session_id, ttl=600
):
    """
    Stores metadata for a Redis stream key to map it to the associated query with a TTL.

    Args:
    - redis_client (redis.RedisCluster): The Redis client instance.
    - stream_key (str): The Redis stream key.
    - query (str): The associated query.
    - class_label (str): classified query label.
    - user_id (str): The user ID associated with the query. Cannot be None.
    - roles (str): The user roles, e.g., json.dumps(["Class3Viewer"]). Cannot be None.
    - session_id (str): The session ID associated with the query. Cannot be None.
    - ttl (int, optional): Time-to-Live in seconds for the metadata.

    Returns:
    - (dict): A dictionary with the status and message of the operation.

    Raises:
    - (ValueError): If user_id, roles, or session_id is None or empty.
    """
    if not (user_id and roles and session_id):
        raise ValueError("user_id, roles, or session_id cannot be None or empty")

    # Create the metadata dictionary
    metadata_key = f"stream:metadata:{stream_key}"
    metadata = {
        "query": query,
        "class_label": class_label,
        "user_id": user_id,
        "roles": roles,
        "session_id": session_id,
    }
    try:
        # Store the metadata in Redis
        redis_client.hset(metadata_key, mapping=metadata)
        redis_client.expire(metadata_key, ttl)
        return {
            "status": "Success",
            "message": "Data stored successfully.",
            "session_id": session_id,
        }
    except Exception as e:
        # Log the exception
        print(f"[Storage] Error storing metadata: {e}")
        return {"status": "Failure", "message": str(e), "session_id": session_id}


def get_user_id_for_session(redis_client, session_id):
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
        user_id_stored = redis_client.hget(session_info_key, "user_id")
        return user_id_stored if user_id_stored else ""
    except Exception as e:
        print(f"[Storage] Error retrieving user ID for session {session_id}: {e}")
        return ""


def get_next_chat_id(redis_client, session_id):
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
        return redis_client.incr(chat_id_key)
    except Exception as e:
        print(f"[Storage] Error incrementing chat ID for session {session_id}: {e}")
        return None


def store_chat_data(
    redis_client,
    session_id,
    user_id="",
    roles="",
    correlation_id="",
    query="",
    answer="",
    sources=[],
    class_label="",
    metadata="",
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
    if metadata == "TutorialMode":
        score = extract_score(answer)
        # if score:
        #     log_event(
        #         "external-query-response-lambda",
        #         "error",
        #         session_id,
        #         user_id,
        #         query,
        #         correlation_id,
        #         "main",
        #         "TEACHme",
        #         f"FINAL USER SCORE: {score}",
        #         score,
        #     )

    try:
        user_sessions_key = f"user:{user_id}:sessions:sorted"
        session_info_key = f"session:{session_id}:info"
        current_time = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
        current_timestamp_str = current_time.isoformat()
        current_timestamp_float = current_time.timestamp()

        if not redis_client.zscore(user_sessions_key, session_id):
            redis_client.zadd(
                user_sessions_key,
                {session_id: current_timestamp_float + 7 * 24 * 60 * 60},
            )
            redis_client.hset(
                session_info_key,
                mapping={
                    "title": query,
                    "latest_timestamp": current_timestamp_str,
                    "user_id": user_id,
                    "roles": roles,
                },
            )
            redis_client.expire(session_info_key, 7 * 24 * 60 * 60)  # 7 days TTL
        else:
            # Validate the user ID associated with the existing session ID
            session_user_id = get_user_id_for_session(redis_client, session_id)
            if session_user_id != user_id:
                raise ValueError("Improper user ID for the provided session ID.")

        chat_id = get_next_chat_id(redis_client, session_id)
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
        redis_client.hset(chat_key, mapping=chat_data)
        redis_client.expire(chat_key, 7 * 24 * 60 * 60)  # 7 days TTL

        # Track chat ID in a list
        redis_client.rpush(f"session:{session_id}:chat_id", chat_id)
        # Trim the sorted set to keep only the latest 20 sessions & 7 days TTL
        redis_client.zremrangebyrank(user_sessions_key, 0, -21)
        redis_client.zremrangebyscore(
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


def get_chat_details_for_session(redis_client, session_id, user_id, n=0):
    """
    Retrieves chat details for a given session ID and user ID.
    This version is using redis pipeline to reduce time & round trips

    Args:
    - redis_client (redis.RedisCluster): The Redis client instance.
    - session_id (str): The unique ID of the session.
    - user_id (str): The unique ID of the user.
    - n (int, optional): The number of chats to retrieve from the chat history.
        If n=0, retrieves whole chat history.

    Returns:
    - (list): A list of chat details for the session.
    """
    # Validate the user ID for the session
    session_info_key = f"session:{session_id}:info"
    session_user_id = redis_client.hget(session_info_key, "user_id")
    if not session_user_id:
        return None
    if session_user_id != user_id:
        raise ValueError("User ID does not match the User ID of the stored session.")

    # Retrieve all chat IDs for the given session ID
    chat_ids = redis_client.lrange(f"session:{session_id}:chat_id", -n, -1)

    # Fetch all chat details in a single pipeline to reduce round trips
    pipeline = redis_client.pipeline()
    for chat_id in chat_ids:
        chat_info_key = f"session:{session_id}:chat:{chat_id}"
        pipeline.hgetall(chat_info_key)
    chat_infos = pipeline.execute()

    # Extract required details using list comprehension
    chats = [
        {
            "chat_id": chat_id,
            "question": chat_info.get("query", ""),
            "answer": chat_info.get("response", ""),
            "sources": chat_info.get("sources", ""),
            "timestamp": chat_info.get("timestamp", ""),
        }
        for chat_id, chat_info in zip(chat_ids, chat_infos)
    ]
    return chats

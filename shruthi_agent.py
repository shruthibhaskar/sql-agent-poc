import asyncio
from input_types import API_Params


async def generate_user_params(params: API_Params):
    async def generate_responses():
        await asyncio.sleep(7)
        yield f"NEW AGENT This is the user input: {params.query}...."
        yield f"NEW AGENT This is the user session_id: {params.session_id}...."
        yield f"NEW AGENT This is the user streaming key: {params.key}...."
        yield f"This is the agent metadata: {params.metadata}...."
        yield f"shruthi_agent SUCCESSFUL"

    # Collect responses and return a list
    return [resp async for resp in generate_responses()]

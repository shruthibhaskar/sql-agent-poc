import asyncio
from input_types import API_Params


async def ben_agent(params: API_Params):
    async def generate_responses():
        await asyncio.sleep(7)
        yield f"This is the user input: {params.query}...."
        yield f"This is the user session_id: {params.session_id}...."
        yield f"This is the user streaming key: {params.key}...."
        yield f"This is the agent metadata: {params.metadata}...."

    # Collect responses and return a list
    return [resp async for resp in generate_responses()]

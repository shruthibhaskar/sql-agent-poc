import os, json, requests, boto3, yaml
from typing_extensions import TypedDict
from langgraph.graph import StateGraph, START, END
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda
from string import Template
from langchain.globals import set_verbose, set_debug
from redis_client import get_redis_client
from input_types import API_Params


# set_verbose(True)
# set_debug(True)
redis_client = get_redis_client()


def load_default_prompt(path="config/prompt_template.yaml"):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"YAML Load Error: {e}")
        return {}


prompts = load_default_prompt()

default_prompt = prompts.get(
    "validate_ppap_prompt", "Enter your Part History prompt here..."
)


class InputState(TypedDict):
    pdf_file: str


class OutputState(TypedDict):
    summary_table: str


class OverallState(InputState, OutputState):
    pages: list


os.environ["no_proxy"] = (
    "10.0.0.0/8,127.0.0.1,localhost,.tycoelectronics.net,169.254.169.254,amazonaws.com"
)
BEDROCK_MODEL_ID = "anthropic.claude-3-5-sonnet-20240620-v1:0"
ocr_url = "http://internal-tel-me-ocr-dev-lb-772432086.us-east-1.elb.amazonaws.com/process-file/"


def load_model():
    return boto3.client("bedrock-runtime", region_name="us-east-1")


def run_ocr(s3_path: str):
    payload = {"s3_path": s3_path}

    response = requests.post(ocr_url, data=payload)
    if response.status_code != 200:
        raise RuntimeError(f"OCR error {response.status_code}: {response.text}")
    return response.json()


def summarize_ppap_workflow(prompt_template: Template):
    graph = StateGraph(OverallState, input=InputState, output=OutputState)

    async def extract_pages(state: InputState) -> OverallState:
        file_hash, _ = state["pdf_file"].split("-", 1)
        redis_hash = f"uploaded-files:{file_hash}"
        file_content = await redis_client.hget(redis_hash, "file_content")

        return {
            "pages": [
                {
                    "page": 1,
                    "headline": "Extracted HTML",
                    "text": file_content,
                }
            ],
            "pdf_file": state["pdf_file"],
            "summary_table": "",
        }

    def summarize_pages(state: OverallState) -> OutputState:
        client = load_model()
        summaries = []

        for page in state["pages"]:
            prompt = prompt_template.substitute(page_text=page["text"])
            response = client.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(
                    {
                        "anthropic_version": "bedrock-2023-05-31",
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 4096,
                        "temperature": 0.0,
                    }
                ),
            )
            content = json.loads(response["body"].read().decode("utf-8"))
            summaries.append(
                content.get("content", [{}])[0].get("text", "No summary generated.")
            )

        return {"summary_table": "\n".join(summaries)}

    graph.add_node("extract_pages", extract_pages)
    graph.add_node("summarize_pages", summarize_pages)
    graph.add_edge(START, "extract_pages")
    graph.add_edge("extract_pages", "summarize_pages")
    graph.add_edge("summarize_pages", END)

    return graph.compile(debug=True)


# Main async wrapper that accepts the prompt string
async def process_ppap_package(params: API_Params):
    filename = params.required_files[0] if params.required_files else "hash-file.pdf"
    prompt_template_str = prompts.get(
        "validate_ppap_prompt", "Enter your Part History prompt here..."
    )

    print("Processing PPAP package...")
    prompt_template = Template(prompt_template_str)
    workflow = summarize_ppap_workflow(prompt_template)
    print("Running workflow:", workflow)
    result = await workflow.ainvoke({"pdf_file": filename})
    chain = (
        RunnableLambda(
            lambda x: f"Summarize the following PPAP content:\n\n{x['summary_table']}"
        )
        | StrOutputParser()
    )
    print("Done Generating summary...")
    return chain.stream({"summary_table": result["summary_table"]})

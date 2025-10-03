
This repository contains TELme agents specific core code

# Setup

```
git clone https://github.com/te-dia/tel-me-agents.git
cd tel-me-agents
python3.12 -m venv base_venv
bash
source base_venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8502
```

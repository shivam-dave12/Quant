from __future__ import annotations
import os
import uvicorn

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("DASHBOARD_PORT", "8000")), reload=False)

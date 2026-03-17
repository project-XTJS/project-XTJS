import uvicorn
import webbrowser
import threading
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

def open_browser():
    """延迟 1.5 秒后自动打开 Swagger 文档页面"""
    url = "http://127.0.0.1:8080/docs"
    webbrowser.open(url)

if __name__ == "__main__":
    threading.Timer(1.5, open_browser).start()

    # 启动 Uvicorn
    uvicorn.run(
        "app.main:app", 
        host="0.0.0.0", 
        port=8080, 
        reload=True,
        log_level="info"
    )
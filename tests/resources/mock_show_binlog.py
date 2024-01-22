"""
Mock server to test the SHOW BINLOG method from endpoint
"""
from fastapi import FastAPI

app = FastAPI()

result_data = {
    "logs": [
        {
            "log_name": "binlog.022361",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022362",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022363",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022364",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022365",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022366",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022367",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022368",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022369",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022370",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022371",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022372",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022373",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022374",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022375",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022376",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022377",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022378",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022379",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022380",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022381",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022382",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022383",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022384",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022385",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022386",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022387",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022388",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022389",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022390",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022391",
            "file_size": "241"
        },
        {
            "log_name": "binlog.022392",
            "file_size": "241"
        },
        {
            "log_name": "binlog.000282",
            "file_size": "0"
        }
    ]
}


@app.get("/show_master")
def show_master():
    return result_data

# run using uvicorn main:app --reload

path = r"C:\dev\nestmatch\app\main.py"
content = open(path).read()
content = content.replace(
    'allow_origins=[\n        "http://localhost:3000",\n        "https://*.vercel.app",\n    ],\n    allow_methods=["POST", "GET"],',
    'allow_origins=["*"],\n    allow_methods=["*"],'
)
open(path, "w").write(content)
print("done")

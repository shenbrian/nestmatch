path = r"C:\dev\nestmatch\app\engine.py"
content = open(path, encoding="utf-8", errors="replace").read()

# Fix the corrupted line
content = content.replace(
    'tradeoffs.append("No nearby train station \x97 bus or car dependent")',
    'tradeoffs.append("No nearby train station - bus or car dependent")'
)
content = content.replace(
    'tradeoffs.append("No nearby train station \ufffd bus or car dependent")',
    'tradeoffs.append("No nearby train station - bus or car dependent")'
)

open(path, "w", encoding="utf-8").write(content)
print("done")

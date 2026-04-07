path = r"C:\dev\nestmatch\app\engine.py"
content = open(path).read()

old = '    dist = prop["distance_to_station_m"]\n    if dist <= 400:\n        highlights.append(f"{dist}m to station \u2014 excellent walkability")\n    elif dist <= 700:\n        highlights.append(f"{dist}m to nearest station")\n    else:\n        tradeoffs.append(f"{dist}m to station \u2014 likely bus-dependent")'

new = '    dist = prop.get("distance_to_station_m")\n    if dist is None:\n        tradeoffs.append("No nearby train station \u2014 bus or car dependent")\n    elif dist <= 400:\n        highlights.append(f"{dist}m to station \u2014 excellent walkability")\n    elif dist <= 700:\n        highlights.append(f"{dist}m to nearest station")\n    else:\n        tradeoffs.append(f"{dist}m to station \u2014 likely bus-dependent")'

if old in content:
    content = content.replace(old, new)
    open(path, "w").write(content)
    print("Fixed successfully")
else:
    # Try a simpler targeted fix
    content = content.replace(
        'dist = prop["distance_to_station_m"]',
        'dist = prop.get("distance_to_station_m")'
    )
    content = content.replace(
        '    if dist <= 400:',
        '    if dist is None:\n        tradeoffs.append("No nearby train station \u2014 bus or car dependent")\n    elif dist <= 400:'
    )
    open(path, "w").write(content)
    print("Fixed with fallback method")

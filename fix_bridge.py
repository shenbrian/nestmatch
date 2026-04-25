with open('bridge_agent_replies.py', 'r', encoding='utf-8') as f:
    content = f.read()
content = content.replace('real_estate_agency AS agency', 'agency')
with open('bridge_agent_replies.py', 'w', encoding='utf-8') as f:
    f.write(content)
print('fixed')

import re
with open('index_railway.html', 'r', encoding='utf-8') as f:
    html = f.read()

if '@keyframes pulse' not in html:
    html = html.replace('</style>', '''
    @keyframes pulse {
      0% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.5; transform: scale(1.2); }
      100% { opacity: 1; transform: scale(1); }
    }
  </style>''')

with open('index_railway.html', 'w', encoding='utf-8') as f:
    f.write(html)

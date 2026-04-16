const fs = require('fs');
let html = fs.readFileSync('index_railway.html', 'utf8');

if (!html.includes('@keyframes pulse')) {
    html = html.replace('</style>', `
    @keyframes pulse {
      0% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.5; transform: scale(1.2); }
      100% { opacity: 1; transform: scale(1); }
    }
  </style>`);
}

fs.writeFileSync('index_railway.html', html);

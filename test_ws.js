const WebSocket = require('ws');
const ws = new WebSocket('wss://web-production-3ff3f.up.railway.app/ws');
ws.on('open', () => console.log('Connected'));
ws.on('message', (data) => {
    const json = JSON.parse(data);
    console.log(JSON.stringify(json.state ? json.state.trader_guide : null, null, 2));
    process.exit(0);
});

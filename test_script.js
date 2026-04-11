const fs = require('fs');
const html = fs.readFileSync('index_railway.html', 'utf8');
const scriptMatch = html.match(/<script>([\s\S]*?)<\/script>/);
if (scriptMatch) {
  let js = scriptMatch[1];
  // mock window and DOM
  js = `
    const location = { protocol: 'https:', host: 'test' };
    function $(id) { return { classList: { toggle:()=>{} }, appendChild:()=>{}, style: {}, querySelector:()=>null, innerHTML: '' }; }
    function txt() {}
    class WebSocket { constructor() {} }
    class ResizeObserver { constructor() {} observe() {} }
    const LightweightCharts = { createChart: () => ({ addCandlestickSeries: () => ({ setData: ()=>{}, update: ()=>{}, removePriceLine: ()=>{}, createPriceLine: ()=>{} }) }) };
    const document = { createElement: ()=>({ classList: {toggle:()=>{}}, className: '', style: {} }), querySelectorAll: ()=>[] };
    
    // We remove connect() so it doesn't actually try to connect
    ${js.replace('connect();', '')}
    
    // Now simulate a WS message
    ws = new WebSocket();
    try {
      ws.onmessage({ data: JSON.stringify({
        type: 'full_update',
        state: { price: 25000 },
        bars: [{time: 1, open: 1, high: 2, low: 1, close: 2}],
        signals: [{name: 'Test', direction: 'long', entry: 25000, confidence_pct: 100}]
      }) });
      console.log('Simulation complete without crash!');
    } catch (e) {
      console.error('CRASH:', e);
    }
  `;
  fs.writeFileSync('sandbox.js', js);
}

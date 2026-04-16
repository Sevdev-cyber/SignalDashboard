const fs = require('fs');
const jsdom = require('jsdom');
const { JSDOM } = jsdom;

const html = fs.readFileSync('index_railway.html', 'utf-8');
const dom = new JSDOM(html, { runScripts: "dangerously", pretendToBeVisual: true });
console.log("JSDOM initialized, checking for errors...");
dom.window.onerror = function(msg, source, line, col, error) {
    console.error("PAGE ERROR:", msg, line, col);
};

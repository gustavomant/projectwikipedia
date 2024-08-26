const fs = require('fs');
const sax = require('sax');
const { stringify } = require('csv-stringify/sync');
const { extract_all_links } = require("./utils/extract_all_links");

const filePath = './assets/ptwiki-20240701-pages-articles-multistream.xml';
const readStream = fs.createReadStream(filePath);

const fileSize = fs.statSync(filePath).size;
let bytesRead = 0;

const saxStream = sax.createStream(true, { lowercase: true });
let pageCount = 0;
//values
let currentTag = null;
let currentPageText = [];
let currentTitle = null;
let currentText = "";
let currentNamespace = ""; 

//trackers
let inPageTag = false;
let inTitleTag = false;
let inTextTag = false;
let inNamespaceTag = false;
const results = [];

saxStream.on("data", (chunk) => {
    bytesRead+=chunk.length;
    const progress = (bytesRead / fileSize) * 100;
    console.log(`Progresso: ${progress.toFixed(2)}%`);
});

saxStream.on('opentag', (node) => {
    currentTag = node.name;

    if (currentTag === 'page') {
        inPageTag = true;
        currentPageText = [];
        currentTitle = null;
        currentText = '';
    }

    if (currentTag === 'title' && inPageTag) {
        inTitleTag = true;
    }

    if (currentTag === 'text' && inPageTag) {
        inTextTag = true;
    }

    if (currentTag === "ns" && inPageTag) {
        inNamespaceTag = true;
    }
});

saxStream.on('text', (text) => {
    if (inPageTag) {
        currentPageText.push(text);
    }

    if (inTitleTag) {
        currentTitle = text.trim();
    }

    if (inTextTag) {
        currentText += text;
    }

    if (inNamespaceTag) {
        currentNamespace = text;
    }
});

saxStream.on('closetag', (tagName) => {
    if (tagName === 'page') {
        if (currentTitle && currentText) {
            pageCount++;
            const links = extract_all_links(currentText);

            if (links.length > 0 && currentNamespace == 0) {
                links.forEach((link, index) => {
                    results.push({
                        source: currentTitle.trim(),
                        target: link.target,
                        index: link.index
                    });
                });
            }
        }

        inPageTag = false;

        if (pageCount >= 10) {
            saxStream.emit('end');
        }
    }

    if (tagName === 'title' && inTitleTag) {
        inTitleTag = false;
    }

    if (tagName === 'text' && inTextTag) {
        inTextTag = false;
    }

    if (tagName === "ns" && inNamespaceTag) {
        inNamespaceTag = false;
    }

    currentTag = null;
});

saxStream.on('end', () => {
    if(results.length > 0) {
        const csv = stringify(results, { header: false, columns: ["source", "target", "index"] });
        fs.appendFileSync("./output/page-redirects.csv", csv, "utf-8");
        results.length = 0;
    }
});

readStream.pipe(saxStream);


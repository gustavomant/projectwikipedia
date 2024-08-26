const fs = require('fs');
const sax = require('sax');
const { stringify } = require('csv-stringify/sync');

const filePath = './assets/ptwiki-20240701-pages-articles-multistream.xml';
const fileSize = fs.statSync(filePath).size;
let bytesRead = 0;

const saxStream = sax.createStream(true, { lowercase: true });

let pageCount = 0;

let currentTag = null;
//values
let currentTitle = null;
let currentNamespace = null;
let currentId = null;
//
let inPageTag = false;
//
let inTitleTag = false;
let inNamespaceTag = false;
let inIdTag = false;
let inRevisionTag = false;

const results = [];

saxStream.on("data", (chunk) => {
    bytesRead+=chunk.length;
    const progress = (bytesRead / fileSize) * 100;
    console.log(`Progresso: ${progress.toFixed(2)}%`);
});

saxStream.on("opentag", (node) => {
    currentTag = node.name;
    if (currentTag === "page") {
        inPageTag = true;
        currentTitle = null;
        currentNamespace = null;
    }

    if (currentTag === "title" && inPageTag) {
        inTitleTag = true;
    }

    if (currentTag === "ns" && inPageTag) {
        inNamespaceTag = true;
    }

    if (currentTag === "revision" && inPageTag) {
        inRevisionTag = true;
    }

    if (currentTag === "id" && inPageTag && !inRevisionTag) {
        inIdTag = true;
    }
});

saxStream.on("text", (text) => {
    if (inTitleTag) {
        currentTitle = text.trim();
    }

    if (inNamespaceTag) {
        currentNamespace = text.trim();
    }

    if (inIdTag) {
        currentId = text.trim();
    }
});

saxStream.on("closetag", (tagname) => {
    if (tagname === "page") {
        pageCount++;
        if(currentNamespace == 0) {
            results.push({id: currentId, title: currentTitle, namespace: currentNamespace});
        }
        inPageTag = false;

        if (pageCount >= 10) {
            saxStream.emit("end");
        }
    }

    if (tagname === "revision" && inRevisionTag) {
        inRevisionTag = false;
    }

    if (tagname === "id" && inIdTag) {
        inIdTag = false;
    }

    if (tagname === "title" && inTitleTag) {
        inTitleTag = false;
    }

    if (tagname === "ns" && inNamespaceTag) {
        inNamespaceTag = false;
    }

    currentTag = null;
});

saxStream.on("end", () => {
    if (results.length > 0) {
        const csv = stringify(results, { header: false, columns: ["id", "title", "namespace"] });
        fs.appendFileSync("titles.csv", csv, "utf-8");
        results.length = 0;
    }
});

const readStream = fs.createReadStream(filePath);
readStream.pipe(saxStream);
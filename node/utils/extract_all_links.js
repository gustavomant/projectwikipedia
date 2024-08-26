const extract_all_links = (text) => {
    const textWithoutParenthesisLinks = text.replace(/\(.*?\)/g, '');
    const regex = /\[\[([^\]|]+)(\|[^\]]+)?\]\]/g;
    const links = [];
    let matches;
    let index = 1;

    while ((matches = regex.exec(textWithoutParenthesisLinks)) !== null) {
        links.push({
            target: matches[1].trim(),
            index: index++
        });
    }

    return links;
};

module.exports = { extract_all_links };

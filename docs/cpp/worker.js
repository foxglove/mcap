importScripts('https://cdn.jsdelivr.net/npm/minisearch@2.4.1/dist/umd/index.min.js');

fetch('index.json').then(function (res) {
    return res.json();
}).then(function (data) {
    var miniSearch = new MiniSearch({
        idField: 'sid',
        fields: ['name', 'decl'], // fields to index for full-text search
        storeFields: ['decl', 'type', 'sid'] // fields to return with search results
    })
    miniSearch.addAll(data);
    postMessage(JSON.stringify(miniSearch.toJSON()));
});

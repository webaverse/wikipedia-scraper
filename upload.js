const path = require('path');
const fs = require('fs');
const readline = require('readline');
const weaviate = require('weaviate-client');
// const mkdirp = require('mkdirp');
const uuidByString = require('uuid-by-string');

//

const schemas = [
  {
    "class": "Paragraph",
    "description": "A Wikipedia article paragraph",
    "properties": [
      {
        "dataType": [
          "string"
        ],
        "description": "Title of the paragraph",
        "name": "title"
      },
      {
        "dataType": [
          "string"
        ],
        "description": "Content of the paragraph",
        "name": "content"
      },
    ],
  },
  {
    "class": "Article",
    "description": "A Wikipedia article",
    "properties": [
      {
        "dataType": [
          "string"
        ],
        "description": "Title of the page",
        "name": "title"
      },
      {
        "dataType": [
          "string[]"
        ],
        "description": "Crefs of the page",
        "name": "crefs"
      },
      {
        "dataType": [
          "Paragraph"
        ],
        "description": "Paragraphs on the page",
        "name": "hasParagraphs"
      },
    ],
  },
];

// console.log('got example', examples[0]);

const batchSize = 100;

const client = weaviate.client({
  scheme: 'http',
  host: 'weaviate-server.webaverse.com',
});
(async () => {
  await client
    .schema
    .getter()
    .do();
  for (const schema of schemas) {
    try {
      await client.schema
        .classCreator()
        .withClass(schema)
        .do();
    } catch(err) {
      if (!/422/.test(err)) { // already exists
        throw err;
      }
    }
  }

  const _formatArticle = v => {
    let {title, paragraphs} = v;
    paragraphs = paragraphs.map(p => {
      let {title: title2 = '', content = ''} = p;
      title2 = `${title}:${title2}`;
      return {
        class: 'Paragraph',
        id: uuidByString(title2),
        properties: {
          title: title2,
          content,
        },
      };
    });
    delete v.paragraphs;
    
    const article = {
      class: 'Article',
      id: uuidByString(title),
      properties: v,
    };

    return {
      article,
      paragraphs,
    };
  };

  const numRetries = 20;
  const _uploadDatas = async datas => {
    const batcher = client.batch.objectsBatcher();
    for (const data of datas) {
      batcher.withObject(data);
    }
    let result = null, err = null;
    try {
      result = await batcher.do();
    } catch(e) {
      err = e;
    }
    if (err === null) {
      let ok = true;
      for (const item of result) {
        if (item.result.errors) {
          console.warn(item.result.errors);
          ok = false;
        }
      }
      return ok;
    } else {
      console.warn(err.stack);
      return false;
    }
  };
  async function processLineByLine() {
    const fileStream = fs.createReadStream('./articles.json');
    fileStream.on('error', err => {
      console.error(err);
    });
  
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    // Note: we use the crlfDelay option to recognize all instances of CR LF
    // ('\r\n') in input.txt as a single line break.
  
    const articleSpecs = [];
    let uploadedParagraphs = 0;
    let uploadedArticles = 0;
    let readLines = 0;
    const _flushArticleSpecs = async () => {
      console.log('flushing article specs', `(${articleSpecs.length} total, ${readLines} lines)`);
      
      const paragraphs = articleSpecs.map(a => a.paragraphs).flat();
      for (let k = 0; k < paragraphs.length; k += 100) {
        const localParagraphs = paragraphs.slice(k, k + 100);
        console.log('flushing paragraphs', localParagraphs.length, `(${uploadedParagraphs} total, ${readLines} lines)`);
        for (let j = 0; j < numRetries; j++) {
          const ok = await _uploadDatas(localParagraphs);
          if (ok) {
            uploadedParagraphs += localParagraphs.length;
            break;
          }
          if (j === numRetries - 1) {
            throw new Error('failed to upload paragraphs');
          }
        }
      }

      const articles = articleSpecs.map(a => a.article).flat();
      console.log('flushing articles', articles.length, `(${uploadedArticles} total, ${readLines} lines)`);
      for (let j = 0; j < numRetries; j++) {
        const ok = await _uploadDatas(articles);
        if (ok) {
          break;
        }
        if (j === numRetries - 1) {
          throw new Error('failed to upload articles');
        }
      }
      uploadedArticles += articles.length;

      articleSpecs.length = 0;
    };
    for await (const line of rl) {
      readLines++;
      // Each line in input.txt will be successively available here as `line`.
      // console.log(`Line from file: ${line}`);
      let j = JSON.parse(line);
      const articleSpec = _formatArticle(j);
      articleSpecs.push(articleSpec);
      if (articleSpecs.length >= batchSize) {
        await _flushArticleSpecs();
      }
    }
    console.log('done reading lines');
    if (articleSpecs.length > 0) {
      await _flushArticleSpecs();
    }
    console.log('done final flush');
  }
  await processLineByLine();
})().catch(err => {
  console.warn(err)
})
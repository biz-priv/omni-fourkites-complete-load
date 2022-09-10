const csv = require('fast-csv')
async function csvToJSON(s3Stream) {
  return new Promise((resolve) => {
    try {
      let data = []
      csv.parseStream(s3Stream, { headers: true })
        .on('data', (row) => {
          data.push(row)
        }).on("end", function () {
          resolve(data)
        })
    } catch (error) {
      reject(error)
    }
  })
}

module.exports = { csvToJSON }
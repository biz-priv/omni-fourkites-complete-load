const excel = require('excel4node');

function generateExcelSheet(array, worksheet, styleForData) {
    console.info("array", array);
    let row = 2;
    for (let i in array) {
        let o = 1;
        worksheet.cell(row, o).string(array[i]['Status']).style(styleForData);
        worksheet.cell(row, o + 1).string(array[i]['Request Params']).style(styleForData);
        worksheet.cell(row, o + 2).string(array[i]['Response']).style(styleForData);
        row = row + 1;
    }
}

async function itemInsertIntoExcel(fourkitesFailedRecordsArray) {
    try {
        let workbook = new excel.Workbook();
        let style = workbook.createStyle({
            font: {
                color: '#47180E',
                size: 12
            },
            numberFormat: '$#,##0.00; ($#,##0.00); -'
        });

        let styleForData = workbook.createStyle({
            font: {
                color: '#47180E',
                size: 10
            },
            alignment: {
                wrapText: true,
                horizontal: 'center',
            },
            numberFormat: '$#,##0.00; ($#,##0.00); -'
        });
        let isDataAvailable = 0;

        if (fourkitesFailedRecordsArray.length > 0) {
            let worksheet1 = workbook.addWorksheet('Child Data');
            worksheet1.cell(1, 1).string('Status').style(style);
            worksheet1.cell(1, 2).string('Request Params').style(style);
            worksheet1.cell(1, 3).string('Response').style(style);
            generateExcelSheet(fourkitesFailedRecordsArray, worksheet1, styleForData)
            isDataAvailable = 1;
        }

        if (isDataAvailable) {
            workbook.write('/tmp/fourkitesFailedRecords.xlsx');
        }
        
    } catch (e) {
        console.error("itemInsert in Excel Error: ", e);
        return e;
    }
}

module.exports = { itemInsertIntoExcel }
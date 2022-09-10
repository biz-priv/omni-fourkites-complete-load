const nodemailer = require("nodemailer");
const fs = require('fs')

async function sendEmail(mailSubject, body) {
    return new Promise(async (resolve, reject) => {
        try {
            const TRANSPORTER = nodemailer.createTransport({
                host: 'email-smtp.us-east-1.amazonaws.com',
                port: '587',
                auth: {
                    user: 'AKIAXLP624BLJQ5JICVQ',
                    pass: 'BFzlzKrkZAIdRqFu3BoxO1uGRNCjqGUfEeZN/JpTaIuV',
                },
            });
            let emailParams = {
                from: 'reports@omnilogistics.com',
                to: 'sujit.das@bizcloudexperts.com',
                subject: "dev" + "-" + mailSubject,
                html: body,
                attachments: [
                    {
                        filename: 'fourkitesFailedRecords.xlsx',
                        path: '/tmp/fourkitesFailedRecords.xlsx'
                    },
                ],
            }
            let sendEmailReport = await TRANSPORTER.sendMail(emailParams);
            console.info('emailSent : ',sendEmailReport);
            await fs.unlinkSync('/tmp/fourkitesFailedRecords.xlsx');
            resolve(true);
        } catch (error) {
            console.error("Send Email Error : \n", error);
            resolve(false);
        }
    })
}


module.exports = { sendEmail }


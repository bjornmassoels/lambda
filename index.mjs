// importeer de specifieke AWS service pakketten die je nodig hebt
import { S3Client, GetObjectCommand, ListObjectsCommand, PutObjectCommand, UploadPartCommand } from "@aws-sdk/client-s3";
import archiver from 'archiver';
import { S3RequestPresigner } from '@aws-sdk/s3-request-presigner';
import { createRequest } from '@aws-sdk/util-create-request';
import { formatUrl } from '@aws-sdk/util-format-url';
import fs from 'fs';
import toArray from 'stream-to-array';

// maak en gebruik v3 service clients, vervang globale waarden zoals region door configuratiewaarden die als argumenten worden doorgegeven aan de client
const s3Client = new S3Client({ region: "eu-north-1" , endpoint: "https://s3.eu-north-1.amazonaws.com" });
export const handler = async (event, context) => {
    try  {
        let archive = archiver('zip', {
            zlib: { level: 9 }
        });
        const zipFileName = event.zipFileName;
        const groupName = event.groupName;
        const projectIds = event.projectIds;
        const slokkerProjectIds = event.slokkerProjectIds;
        // List all objects from a bucket

        const inputBucket = 'selux-riolering-pdf-saver';

        let output = fs.createWriteStream('/tmp/' + zipFileName);

        archive.on('error', function(err) {
            console.log('Archive error:', err);
            throw err;
        });
// good practice to catch warnings (ie stat failures and other non-blocking errors)
        archive.on('warning', function(err) {
            if (err.code === 'ENOENT') {
                // log warning
                console.log(err)
            } else {
                // throw error
                console.log(err)
                throw err;
            }
        });
        console.log(groupName)

        archive.pipe(output);

        let proccesProjects = await processPDFs(event.projectIds, event.groupName, inputBucket, archive, s3Client);
        if(proccesProjects?.status === 'error') {
            return { statusCode: 500 };
        }
        // Process slokkerProjectIds PDFs, 2 at a time
        let processSlokkerProjects = await processPDFs(event.slokkerProjectIds, event.groupName, inputBucket, archive, s3Client);
        if(processSlokkerProjects?.status === 'error') {
            return { statusCode: 500 };
        }
        console.log('before finalize');
        return await new Promise((resolve, reject) => {
            output.on('finish', async () => {
                console.log(archive.pointer() + ' total bytes');
                // Now upload the file to S3
                const filePath = '/tmp/' + zipFileName;
                const fileStream = fs.createReadStream(filePath);
                const outputParams = {
                    Bucket: "pdfzipbucket",
                    Key: zipFileName,
                    Body: fileStream,
                    ContentType: 'application/zip'
                };
                console.log('before send2')
                try {
                    await s3Client.send(new PutObjectCommand(outputParams));
                    console.log('after send');
                    const signer = new S3RequestPresigner(s3Client.config);
                    const request = await createRequest(s3Client, new GetObjectCommand({
                        Bucket: outputParams.Bucket,
                        Key: outputParams.Key
                    }));
                    const signedUrl = formatUrl(await signer.presign(request));
                    console.log('signedUrl:', signedUrl);
                    resolve({ statusCode: 200, url: signedUrl });
                    return { statusCode: 200, url: signedUrl };
                } catch (ex) {
                    reject(ex);
                }
            });

            archive.finalize().then(() => {
                console.log('finalize done');
            }).catch(reject);  // Catch any errors that might occur during archive.finalize()
        }).catch((ex) => {
            console.log(ex);
            return { statusCode: 500 };
        });

    } catch (ex) {
        console.log(ex);
        return { statusCode: 500 };
    }
};


async function processPDFs(projectIds, groupName, inputBucket, archive, s3Client) {
    for (let i = 0; i < projectIds.length; i++) {
        const file = projectIds[i] + '.pdf';
        const inputParams = {
            Bucket: inputBucket,
            Key: groupName + '/' + file,
        };
        try {
            console.log(`Fetching ${file}`);
            const inputData = await s3Client.send(new GetObjectCommand(inputParams));

            // Convert the stream to a Buffer
            const arr = await toArray(inputData.Body);
            const buffer = Buffer.concat(arr);

            // Write the Buffer to a local file
            fs.writeFileSync('/tmp/' + file, buffer);

            console.log(`Successfully fetched ${file}`);

            await archive.append(fs.createReadStream('/tmp/' + file), { name: file});
        } catch (err) {
            console.log(`Error getting object ${file} from bucket:`, err);
            return {status: 'error'}
        }

        // Sleep for 1 second (1000 milliseconds) before next iteration
    }
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

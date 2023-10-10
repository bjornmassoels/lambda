// importeer de specifieke AWS service pakketten die je nodig hebt
import { S3Client, GetObjectCommand, ListObjectsCommand, PutObjectCommand, UploadPartCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import archiver from 'archiver';
import streamToPromise from 'stream-to-promise';
import { S3RequestPresigner } from '@aws-sdk/s3-request-presigner';
import { createRequest } from '@aws-sdk/util-create-request';
import { formatUrl } from '@aws-sdk/util-format-url';

// maak en gebruik v3 service clients, vervang globale waarden zoals region door configuratiewaarden die als argumenten worden doorgegeven aan de client
const s3Client = new S3Client({ region: "eu-north-1" , endpoint: "https://s3.eu-north-1.amazonaws.com" });
export const handler = async (event, context) => {
    try  {
        let archive = archiver('zip', {
            gzip:true,
            zlib: { level: 9 }
        });
        const zipFileName = event.zipFileName;
        const groupName = event.groupName;
        const projectIds = event.projectIds;
        const slokkerProjectIds = event.slokkerProjectIds;
        // List all objects from a bucket

        const inputBucket = 'selux-riolering-pdf-saver';


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
        let endedStreams = 0;
        const listObjectsInput = {
            Bucket: inputBucket,
            Prefix: groupName + "/"
        };
        console.log(zipFileName);
        console.log(groupName)

        let { Contents } = await s3Client.send(new ListObjectsCommand(listObjectsInput));
        console.log('Contents:', Contents);
        if (!Contents) {
            new Error("Contents is not defined.");
        }



        for(let file of projectIds){
            file = file + '.pdf';
            const inputParams = {
                Bucket: inputBucket,
                Key: groupName + '/' + file
            };
            let inputData;

            try {
                console.log(file)
                try {
                    inputData = await s3Client.send(new GetObjectCommand(inputParams));
                } catch (err) {
                    console.log("Error fetching PDF:", err);
                    throw err;
                }
                archive.append(inputData.Body, { name: file});
            } catch (err) {
                console.log(`Error getting object ${file} from bucket:`, err);
                continue;
            }
        }


        for(let file of slokkerProjectIds){
            file = file + '.pdf';
            const inputParams = {
                Bucket: inputBucket,
                Key: groupName + '/' + file
            };
            let inputData;

            try {
                console.log(file)
                try {
                    inputData = await s3Client.send(new GetObjectCommand(inputParams));
                } catch (err) {
                    console.log("Error fetching PDF:", err);
                    throw err;
                }
                archive.append(inputData.Body, { name: file});
            } catch (err) {
                console.log(`Error getting object ${file} from bucket:`, err);
                continue;
            }
        }

        console.log('before close init');

        const endArchive = new Promise(async (resolve, reject) => {
                console.log('Archive ended');
                try {
                    const outputParams = {
                        Bucket: "pdfzipbucket",
                        Key: zipFileName,
                        Body: await streamToPromise(archive),
                        ContentType: 'application/zip'
                    };

                    s3Client.send(new PutObjectCommand(outputParams)).then(async (data) => {
                        console.log('after send');
                        const signer = new S3RequestPresigner(s3Client.config);
                        const request = await createRequest(s3Client, new GetObjectCommand({
                            Bucket: outputParams.Bucket,
                            Key: outputParams.Key
                        }));
                        const signedUrl = formatUrl(await signer.presign(request));
                        console.log('signedUrl:', signedUrl);
                        resolve({ statusCode: 200, url: signedUrl });
                    });
                } catch (error) {
                    // error handling.
                    console.log(error);
                    reject(error);
                } finally {
                    // finally.
                    console.log("finalize");
                }
        });
        console.log('before finalize');
        await archive.finalize();
        console.log('after finalize');
        // Now you can await this promise.
        try {
            const result = await endArchive;
            console.log(result);
            return result;
        } catch (error) {
            console.log(error);
        }

    } catch (ex) {
        console.log(ex);
        return { statusCode: 500 };
    }
};

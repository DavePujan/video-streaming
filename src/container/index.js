import fs from "fs/promises";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import path from "path";
import ffmpeg from "fluent-ffmpeg";

const RESOLUTION = [
    { name: "240p", width: 480, height: 360 },
    { name: "360p", width: 858, height: 480 },
    { name: "720p", width: 1280, height: 720 }
];

const tempBucketClient = new S3Client({
    region: "ap-south-1",
    endpoint: "https://s3.ap-south-1.amazonaws.com",
    credentials: {
        accessKeyId: "XXXXXXXXXXXXXX",
        secretAccessKey: "XXXXXXXXXXXXXXX"
    }
});

const productionBucketClient = new S3Client({
    region: "us-east-1",
    endpoint: "https://s3.us-east-1.amazonaws.com",
    credentials: {
        accessKeyId: "XXXXXXXXXXXXX",
        secretAccessKey: "XXXXXXXXXXXXXXX"
    }
});

const BUCKET_NAME = process.env.BUCKET_NAME;
const KEY = process.env.KEY;

async function init() {
    // Download the original video from S3
    const command = new GetObjectCommand({
        Bucket: BUCKET_NAME,
        Key: KEY
    });

    const result = await tempBucketClient.send(command);
    const originalfilepath = `original-video.mp4`;
    await fs.writeFile(originalfilepath, result.Body);

    const originalVideoPath = path.resolve(originalfilepath);

    // Start transcoding
    const Promises = RESOLUTION.map(async (resolution) => {
        const output = `video-${resolution.name}.mp4`;
        return new Promise((resolve) => {
            ffmpeg(originalVideoPath)
                .output(output)
                .videoCodec("libx264")
                .audioCodec("aac")
                .withSize(`${resolution.width}x${resolution.height}`)
                .on("start", () => {
                    console.log("start", `${resolution.width}x${resolution.height}`);
                })
                .on("end", async () => {
                    const putCommand = new PutObjectCommand({
                        Bucket: "production.poojandave.dev",
                        Key: output,
                        Body: await fs.readFile(output),
                    });
                    await productionBucketClient.send(putCommand);
                    console.log(`Uploaded ${output}`);
                    resolve();
                })
                .format("mp4")
                .run();
        });
    });

    await Promise.all(Promises);
}

init();
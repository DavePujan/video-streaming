import { ReceiveMessageCommand, SQSClient , DeleteMessageCommand} from "@aws-sdk/client-sqs";
import { ECSClient,RunTaskCommand } from "@aws-sdk/client-ecs";
import type {S3Event} from "aws-lambda";


const sqsClient = new SQSClient({ 
    credentials: {
        accessKeyId: "AKIAU72LF4JRKEDAKCOL",
        secretAccessKey: "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"
    },
    region: "ap-south-1" 
});

const ecsClient = new ECSClient({
    credentials: {
        accessKeyId: "AKIAU72LF4JRKEDAKCOL",
        secretAccessKey: "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"
    },
    region: "ap-south-1" 
})

async function init() {
    // to receive messages from the queue
    const command = new ReceiveMessageCommand({
        QueueUrl: "https://sqs.ap-south-1.amazonaws.com/343218184802/tempRawVideoS3Queue",
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,
    });

    // Infinite loop to poll messages from the queue
    while (true) {
        try {
            const {Messages} = await sqsClient.send(command);
            if (!Messages) {
                console.log("No messages found");
                await new Promise(resolve => setTimeout(resolve, 1000));
                continue;
            }

            for (const message of Messages) {
                const {Body, MessageId} = message;
                console.log("Received message", {MessageId, Body});

                //validate the message
                if (!Body) {
                    continue;
                }

                //validate and parse the event
                const event = JSON.parse(Body) as S3Event;

                //skip the test event
                if ("Service" in message && "Event" in message) {
                    if (message.Event === "s3.TestEvent") { 
                        await sqsClient.send(new DeleteMessageCommand({
                            QueueUrl: "https://sqs.ap-south-1.amazonaws.com/343218184802/tempRawVideoS3Queue",
                            ReceiptHandle: message.ReceiptHandle,
                        }));
                        continue;
                    }
                }
                
                //spin the docker container
                for (const record of event.Records) {
                    const {s3} = record;
                    const {bucket, object:{key}} = s3;

                    const runTaskCommand = new RunTaskCommand({
                        taskDefinition: 
                        'arn:aws:ecs:ap-south-1:343218184802:task-definition/video-transcoder',
                        cluster: 
                        'arn:aws:ecs:ap-south-1:343218184802:cluster/dev',
                        launchType:
                        "FARGATE",
                        networkConfiguration: {
                            awsvpcConfiguration: {
                                assignPublicIp: 'ENABLED',
                                securityGroups: ['sg-0f25b6d6838534c23'],
                                subnets: ['subnet-0bb709adccab0558d','subnet-0e429c145b4ef7206','subnet-070e4bcb57d1e79b9']
                            },
                        },
                        overrides:{
                            containerOverrides: [{
                                name:"video-transcoder", 
                                environment:[{
                                    name: "BUCKET_NAME",value: bucket.name } , {name: 'KEY',value: key}]
                                }]
                        }
                    });

                    await ecsClient.send(runTaskCommand);
                    await sqsClient.send(new DeleteMessageCommand({
                        QueueUrl: "https://sqs.ap-south-1.amazonaws.com/343218184802/tempRawVideoS3Queue",
                        ReceiptHandle: message.ReceiptHandle,
                    }));
                }
                
                //delete the message from the queue
            }
        } catch (error) {
            console.error("Error processing message", error);
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
}

init();

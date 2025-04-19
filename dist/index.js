"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const client_sqs_1 = require("@aws-sdk/client-sqs");
const client_ecs_1 = require("@aws-sdk/client-ecs");
const sqsClient = new client_sqs_1.SQSClient({
    credentials: {
        accessKeyId: "XXXXXXXXXX",
        secretAccessKey: "XXXXXXXXXXX"
    },
    region: "ap-south-1"
});
const ecsClient = new client_ecs_1.ECSClient({
    credentials: {
        accessKeyId: "XXXXXXXXXXX",
        secretAccessKey: "XXXXXXXXXXX"
    },
    region: "ap-south-1"
});
function init() {
    return __awaiter(this, void 0, void 0, function* () {
        // to receive messages from the queue
        const command = new client_sqs_1.ReceiveMessageCommand({
            QueueUrl: "https://sqs.ap-south-1.amazonaws.com/343218184802/tempRawVideoS3Queue",
            MaxNumberOfMessages: 1,
            WaitTimeSeconds: 20,
        });
        // Infinite loop to poll messages from the queue
        while (true) {
            try {
                const { Messages } = yield sqsClient.send(command);
                if (!Messages) {
                    console.log("No messages found");
                    yield new Promise(resolve => setTimeout(resolve, 1000));
                    continue;
                }
                for (const message of Messages) {
                    const { Body, MessageId } = message;
                    console.log("Received message", { MessageId, Body });
                    //validate the message
                    if (!Body) {
                        continue;
                    }
                    //validate and parse the event
                    const event = JSON.parse(Body);
                    //skip the test event
                    if ("Service" in message && "Event" in message) {
                        if (message.Event === "s3.TestEvent") {
                            yield sqsClient.send(new client_sqs_1.DeleteMessageCommand({
                                QueueUrl: "https://sqs.ap-south-1.amazonaws.com/343218184802/tempRawVideoS3Queue",
                                ReceiptHandle: message.ReceiptHandle,
                            }));
                            continue;
                        }
                    }
                    //spin the docker container
                    for (const record of event.Records) {
                        const { s3 } = record;
                        const { bucket, object: { key } } = s3;
                        const runTaskCommand = new client_ecs_1.RunTaskCommand({
                            taskDefinition: 'arn:aws:ecs:ap-south-1:343218184802:task-definition/video-transcoder',
                            cluster: 'arn:aws:ecs:ap-south-1:343218184802:cluster/dev',
                            launchType: "FARGATE",
                            networkConfiguration: {
                                awsvpcConfiguration: {
                                    assignPublicIp: 'ENABLED',
                                    securityGroups: ['XXXXXXXXXX'],
                                    subnets: ['subnet-XXX', 'subnet-XXX', 'subnet-XXX']
                                },
                            },
                            overrides: {
                                containerOverrides: [{
                                        name: "video-transcoder",
                                        environment: [{
                                                name: "BUCKET_NAME", value: bucket.name
                                            }, { name: 'KEY', value: key }]
                                    }]
                            }
                        });
                        yield ecsClient.send(runTaskCommand);
                        yield sqsClient.send(new client_sqs_1.DeleteMessageCommand({
                            QueueUrl: "https://sqs.ap-south-1.amazonaws.com/343218184802/tempRawVideoS3Queue",
                            ReceiptHandle: message.ReceiptHandle,
                        }));
                    }
                    //delete the message from the queue
                }
            }
            catch (error) {
                console.error("Error processing message", error);
                yield new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    });
}
init();

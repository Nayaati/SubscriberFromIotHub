const { EventHubConsumerClient } = require("@azure/event-hubs");
const { MongoClient } = require("mongodb");
const WebSocket = require("ws");
require("dotenv").config();


const eventHubConnectionString = process.env.EVENT_HUB_CONNECTION_STRING;
const eventHubName = process.env.EVENT_HUB_NAME;
const consumerGroup = process.env.ConsumerGroup;

const mongoUri = process.env.MONGO_URI;
const dbName = process.env.MONGO_DB_NAME;
const collectionName = process.env.MONGO_COLLECTION;

const PORT = 8080;
const wss = new WebSocket.Server({ port: PORT });
let clients = [];

wss.on("connection", (ws) => {
    console.log("WebSocket client connected.");
    clients.push(ws);

    ws.on("close", () => {
        clients = clients.filter((c) => c !== ws);
        console.log("WebSocket client disconnected.");
    });
});

async function processEvent(eventData, mongoCollection) {
    const data = eventData.body;

    const { deviceId, temperature, humidity } = data;

    const isAlert = temperature > 30 || humidity > 75;

    const doc = {
        deviceId: deviceId || "unknown",
        temperature: temperature ?? null,
        humidity: humidity ?? null,
        messageType: isAlert ? "ALERT" : "NORMAL",
        timestamp: new Date(),
    };
    await mongoCollection.insertOne(doc);

    console.log(
        `[${doc.messageType}] Device ${doc.deviceId} | Temp: ${doc.temperature} | Humidity: ${doc.humidity}`
    );

    clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(doc));
        }
    });
}
async function main() {

    const mongoClient = new MongoClient(mongoUri);
    await mongoClient.connect();
    const db = mongoClient.db(dbName);
    const collection = db.collection(collectionName);

    console.log("Connected to MongoDB");
    console.log(`WebSocket server listening on ws://localhost:${PORT}`);

    const consumerClient = new EventHubConsumerClient(
        consumerGroup,
        eventHubConnectionString,
        eventHubName
    );

    console.log("Listening to Event Hub...");

    await consumerClient.subscribe({
        processEvents: async (events) => {
            for (const event of events) {
                try {
                    await processEvent(event, collection);
                } catch (err) {
                    console.error("Error saving event:", err);
                }
            }
        },
        processError: async (err) => {
            console.error("Event Hub error:", err);
        },
    });
}

main().catch(console.error);
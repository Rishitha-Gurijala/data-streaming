const { mongoConnect } = require("./utility/mongoConnect.js");
let amqp = require("amqplib");
const dotenv = require("dotenv");
const db = require("./mysqldb");

dotenv.config();
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const queue = process.env.queue;

async function pushToQueue(req, res) {
  const today = new Date().toISOString().split("T")[0];
  let [docs] = await db
    .promise()
    .query(`SELECT *  FROM rides where created_date = '${today}';`);
  if (docs.length) {
    for (let doc of docs) {
      await publishMessage(doc);
    }
    return res.status(200).json({ status: "queued" });
  } else {
    return res.status(200).json({ status: "no messages to push!" });
  }
}

async function updateDB(req, res) {
  let mongodb = await mongoConnect();
  const today = new Date().toISOString().split("T")[0];
  let [docs] = await db
    .promise()
    .query(`SELECT *  FROM rides where created_date = '${today}';`);
  for (let doc of docs) {
    let transformedField = getEpochTime(doc.created_date, doc.ride_time);
    doc[`epoch_time(transformed_field)`] = transformedField;
  }
  await mongodb.collection("rides").insertMany(docs);
  return res.status(200).json({ status: "inserted" });
}

async function getDataFromMongo(req, res) {
  let userId = req.params.userId;
  let mongodb = await mongoConnect();

  let data = await mongodb.collection("rides").find({userId}).toArray();;
  return res.status(200).json({ data: data });
}

function getEpochTime(created_date, ride_time) {
  const date = new Date(created_date);
  const [hours, minutes, seconds] = ride_time.split(":").map(Number);
  date.setUTCHours(hours, minutes, seconds, 0);
  const epochTimeMs = date.getTime();
  return epochTimeMs;
}

async function publishMessage(message) {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    await channel.assertQueue(queue, { durable: true });
    channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
      persistent: true,
    });
    console.log("Message sent:", message.userId);
    setTimeout(() => {
      connection.close();
    }, 500);
  } catch (error) {
    console.error("Error publishing message:", error);
  }
}

module.exports = {
  pushToQueue,
  updateDB,
  getDataFromMongo,
};

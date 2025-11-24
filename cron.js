const { mongoConnect } = require("./utility/mongoConnect.js");
let amqp = require("amqplib");
const dotenv = require("dotenv");
const db = require("./mysqldb");

const { StreamExecutionEnvironment } = require("flink");
let { createClient } = require("@clickhouse/client");

const clickhouseClient = createClient({
  host: "http://localhost:8123",
  database: "default",
});

dotenv.config();
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const queue = process.env.queue;

async function processData() {
  try {
    let mongodb = await mongoConnect();
    let data = await mongodb.collection("rides").find({}).toArray();

    await clickhouseClient.insert({
      table: "rides",
      values: data,
      format: "JSONEachRow",
    });
    console.log("Data successfully transferred to ClickHouse");
  } catch (error) {
    console.error("Error processing data:", error);
  }
}

module.exports = {
  processData,
};

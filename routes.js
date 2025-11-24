const { pushToQueue, updateDB , getDataFromMongo} = require("./controllers.js");

function getRoutes() {
  // user
  app.post("/push", pushToQueue);
  app.post("/updateDB", updateDB);
  app.get("/pull/:userId", getDataFromMongo);
}

module.exports = {
  getRoutes,
};

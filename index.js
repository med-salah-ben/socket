const express = require("express");
const { createServer } = require("node:http");
const { join } = require("node:path");
const { Server } = require("socket.io");
const sqlite3 = require("sqlite3");
const { open } = require("sqlite");
const { availableParallelism } = require("node:os");
const cluster = require("node:cluster");
const { createAdapter, setupPrimary } = require("@socket.io/cluster-adapter");
require("dotenv").config();

async function initDB() {
  const db = await open({ filename: "chat.db", driver: sqlite3.Database });
  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_offset TEXT UNIQUE,
        content TEXT
    );
  `);
  return db;
}

if (cluster.isPrimary) {
  const numCPUs = Math.min(availableParallelism(), 4);
  // create one worker per available core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i,
    });
  }

  // set up the adapter on the primary thread
  return setupPrimary();
}

async function main() {
  const db = await initDB();
  await db.exec("PRAGMA journal_mode=WAL");

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    //Connection state recovery
    connectionStateRecovery: {},
    // set up the adapter on each worker thread
    adapter: createAdapter(),
  });

  app.get("/", (req, res) => {
    res.sendFile(join(__dirname, "index.html"));
  });

  io.on("connection", async (socket) => {
    console.log("a user connected");
    console.log(`🆕 New session: ${socket.id}`);

    // socket.on("ping send", (count) => {
    //   console.log("ping count receive : ", count);
    // });

    socket.on("disconnect", () => {
      console.log("user disconnected");
    });

    // join the room
    socket.join("chat");

    //Broadcasting
    socket.broadcast
      .timeout(5000)
      .emit("user joined", { id: socket.id }, (err, responses) => {
        if (err) {
          console.log("⚠️ Some clients did not respond in time");
        } else {
          console.log("✅ Clients responded:", responses);
        }
      });
    // Rooms
    socket.on("chat message", async (msg, targetId, clientOffset, callback) => {
      let result;
      try {
        // store the message in the database
        result = await db.run(
          "INSERT INTO messages (content, client_offset) VALUES (?, ?)",
          msg,
          clientOffset
        );
        console.log("🚀 ~ main ~ result:", result);
      } catch (e) {
        if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
          console.log("🚀 ~ main ~ 19:", 19);
          // the message was already inserted, so we notify the client
          callback("got it");
        } else {
          // nothing to do, just let the client retry
          console.log("🚀 ~ main ~ 20:", 20);
        }
        return;
      }
      if (targetId) {
        //Join room with targetId to see the msg from the sender
        socket.join(targetId);
        io.to(targetId).emit(
          "chat message",
          { id: socket.id, msg },
          result.lastID
        );
        // io.except(userId).emit("chat message", msg);
        // socket.leave(targetId);
      } else {
        io.emit("chat message", { id: socket.id, msg }, result.lastID);
      }
      callback("got it");
    });
    if (!socket.recovered) {
      console.log("🚀 ~ main ~ handshake:", socket.handshake.auth);
      // if the connection state recovery was not successful
      try {
        await db.each(
          "SELECT id, content FROM messages WHERE id > ?",
          [socket.handshake.auth.serverOffset || 0],
          (_err, row) => {
            const id = row.id;
            const msg = row.content;

            socket.emit("chat message", { id, msg });
          }
        );
      } catch (e) {
        // something went wrong
      }
    }
    //Basic emit
    socket.on("hello", (arg1, arg2, arg3) => {
      // console.log(arg1);
      // console.log(arg2);
      // console.log(arg3);
    });
    //Acknowledgements
    socket
      .timeout(5000)
      .emit("request", { foo: "bar" }, "baz", (err, response) => {
        if (err) {
        } else {
          console.log(response.status); // 'ok'
        }
      });
    //Acknowledgements With a Promise
    socket.on("requestB", (arg1, arg2, callback) => {
      console.log("Server received:", arg1, arg2);
      callback({ status: "ok" });
    });
    // Catch-all listeners
    // This fires for every event received from the client
    socket.onAny((eventName, ...args) => {
      // console.log("➡️ Catch eventName : ", eventName); // 'hello'
      // console.log("➡️ Catch ARGS : ", args); // [ 1, '2', { 3: '4', 5: ArrayBuffer (1) [ 6 ] } ]
    });
    // This fires for every event sent to the client
    socket.onAnyOutgoing((eventName, ...args) => {
      // console.log("➡️ Catch Outgoing:", eventName, args);
    });
  });

  // each worker will listen on a distinct port
  const port = process.env.PORT;

  server.listen(port, () => {
    console.log(`server running at http://localhost:${port}`);
  });
}

main();

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
        content TEXT,
        targetName TEXT,
        senderName TEXT,
        namespace TEXT
    );
  `);
  await db.exec(`
  CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
  //Namespaces

  // io.of("/orders").on("connection", (socket) => {
  //   socket.on("order:list", () => {});
  //   socket.on("order:create", () => {});
  // });

  // io.of("/users").on("connection", (socket) => {
  //   socket.on("user:list", () => {});
  // });

  const nps1 = io.of("/test1");
  const nps2 = io.of("/test2");

  // const nps3 = io.of("/test3");
  const userSockets = new Map(); // socketId -> username

  // //Namespaces
  nps1.on("connection", (socket) => {
    console.log(`[io] connected ${socket.id} to nps1`);
    socket.on("disconnect", () => {
      console.log("User disconnected from /test1");
    });
    socket.emit("welcome", "Welcome to the test1 namespace!");

    socket.on("chatMessage", (content) => {
      console.log("Chat message:", content);
      // Broadcast to everyone in /chat
      nps1.emit("chatMessage", {content});
    });

  });

  nps2.on("connection", async (socketNsp2) => {
    let currentUsername = null;

    socketNsp2.on("login", async (userName) => {
      console.log(`user ${userName} login`);
      currentUsername  = userName;
      userSockets.set(socketNsp2.id, userName);

      socketNsp2.join(userName);

      await db.run("INSERT OR IGNORE INTO users (username) VALUES (?)", [
        userName,
      ]);
      console.log(`[io] connected ${userName} to nps2`);
      socketNsp2.emit("welcome", "Welcome to the test2 panel!");
      // await db.exec("DROP TABLE IF EXISTS messages;");

      const filtredMsg = await db.all(
        `SELECT id, content, namespace, targetName, senderName
       FROM messages
       WHERE namespace = ?
       AND (targetName = ? OR senderName = ? OR targetName IS NULL)
       ORDER BY id ASC`,
        [nps2.name, userName, userName]
      );
      console.log("get message filtredMsg: " + filtredMsg);

      const lastID =
        filtredMsg.length > 0 ? filtredMsg[filtredMsg.length - 1].id : null;
      console.log("✅ nsp2 lastID respond:", lastID);
      socketNsp2.emit("get msg", filtredMsg, lastID);
    });

    // socketNsp2.on("alert", (data) => {
    //   console.log("test2 alert:", data);
    //   nps2.emit("alert", `test2 says: ${data}`);
    // });

    socketNsp2.on("disconnect", () => {
      console.log(`user ${currentUsername} disconnected from /test2`);
      userSockets.delete(socketNsp2.id);
    });

    socketNsp2.on(
      "chat message nsp2",
      async (msg, targetName, clientOffset, namespace, callback) => {
        console.log("✅ nsp2 msg respond:", {
          msg,
          targetName,
          clientOffset,
          namespace,
        });
        const senderName = userSockets.get(socketNsp2.id);
        if (!senderName) {
          console.log("user not logged in");
          return;
        }
        let result;

        try {
          // await db.exec(`ALTER TABLE messages ADD COLUMN targetName TEXT;`);
          // await db.exec(`ALTER TABLE messages ADD COLUMN namespace TEXT;`);
          // await db.exec(`ALTER TABLE messages ADD COLUMN senderName TEXT;`);

          // // store the message in the database
          result = await db.run(
            "INSERT INTO messages (content, client_offset, targetName, senderName, namespace ) VALUES (?, ?, ?, ?, ?)",
            msg,
            clientOffset,
            targetName,
            senderName,
            namespace
          );
          console.log("🚀 ~ result:", result);
        } catch (e) {
          if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
            // console.log("duplicate message, skipping");
          }
          // the message was already inserted, so we notify the client
          // callback("got it");
          return;
        }
        const lastMsg = await db.get(
          "SELECT id, content, namespace, targetName, senderName FROM messages WHERE id = ?",
          [result.lastID]
        );
        if (targetName) {
          //Join room with targetName to see the msg from the sender
          console.log(`sending msg to ${targetName} from ${senderName}`);

          nps2.to(targetName).emit("chat message nsp2", lastMsg, lastMsg.id);
          if (targetName !== senderName) {
            socketNsp2.emit("chat message nsp2", lastMsg, lastMsg.id);
          }
          // io.except(userId).emit("chat message", msg);
          // socket.leave(targetName);
        } else {
          // Broadcast message: send to everyone
          nps2.emit("chat message nsp2", lastMsg, lastMsg.id);
        }

        // callback("got it");
      }
    );
  });

  app.get("/", (req, res) => {
    res.sendFile(join(__dirname, "login.html"));
  });
  app.get("/test", (req, res) => {
    res.sendFile(join(__dirname, "index.html"));
  });
  app.get("/test1", (req, res) => {
    res.sendFile(join(__dirname, "nsp1.html"));
  });
  app.get("/test2", (req, res) => {
    res.sendFile(join(__dirname, "nsp2.html"));
  });
  io.on("connection", async (socket) => {
    console.log("a user connected");
    // console.log(`🆕 New session: ${socket.id}`);

    // socket.on("ping send", (count) => {
    //   console.log("ping count receive : ", count);
    // });
    socket.on("disconnect", () => {
      console.log(`user  disconnected from Main Chat`);
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
    socket.on(
      "chat message",
      async (msg, targetName, clientOffset, callback) => {
        let result;
        try {
          // store the message in the database
          result = await db.run(
            "INSERT INTO messages (content, client_offset) VALUES (?, ?)",
            msg,
            clientOffset
          );
        } catch (e) {
          if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
            // the message was already inserted, so we notify the client
            callback("got it");
          } else {
            // nothing to do, just let the client retry
          }
          return;
        }
        if (targetName) {
          //Join room with targetName to see the msg from the sender
          socket.join(targetName);
          io.to(targetName).emit(
            "chat message",
            { id: socket.id, msg },
            result.lastID
          );
          // io.except(userId).emit("chat message", msg);
          // socket.leave(targetName);
        } else {
          io.emit("chat message", { id: socket.id, msg }, result.lastID);
        }
        callback("got it");
      }
    );

    if (!socket.recovered) {
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

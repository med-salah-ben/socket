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
        namespace TEXT,
        created_at INTEGER DEFAULT (strftime('%s', 'now'))

    );
    -- Add indexes for faster queries
  CREATE INDEX IF NOT EXISTS idx_namespace ON messages(namespace);
  CREATE INDEX IF NOT EXISTS idx_target ON messages(targetName);
  CREATE INDEX IF NOT EXISTS idx_sender ON messages(senderName);
  CREATE INDEX IF NOT EXISTS idx_created ON messages(created_at);
  `);
  await db.exec(`
  CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  `);
  // Enable WAL mode for better concurrent access
  await db.exec("PRAGMA journal_mode=WAL");
  await db.exec("PRAGMA synchronous=NORMAL"); // Faster writes
  await db.exec("PRAGMA cache_size=10000"); // Larger cache
  return db;
}

if (cluster.isPrimary) {
  // set up the adapter on the primary BEFORE forking
  setupPrimary();

  const numCPUs = Math.min(availableParallelism(), 4);
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({ PORT: 3000 + i });
  }
  return; // primary exits main(); workers run it
}

async function main() {
  const db = await initDB();
  const insertMessageStmt = await db.prepare(
    "INSERT INTO messages (content, client_offset, targetName, senderName, namespace) VALUES (?, ?, ?, ?, ?)"
  );
  await db.exec("PRAGMA journal_mode=WAL");

  const app = express();

  const server = createServer(app);
  const io = new Server(server, {
    //Connection state recovery
    connectionStateRecovery: {},
    // set up the adapter on each worker thread
    adapter: createAdapter(),
    // Add sticky session support
    transports: ["websocket", "polling"],
    allowEIO3: true,
  });
  //Namespaces

  // io.of("/orders").on("connection", (socket) => {
  //   socket.on("order:list", () => {});
  //   socket.on("order:create", () => {});
  // });

  // io.of("/users").on("connection", (socket) => {
  //   socket.on("user:list", () => {});
  // });
  // const nsp0 = io.of("/test-0");
  // const nps1 = io.of("/test-1");
  // const nps2 = io.of("/test-2");
  const dynamicNsp = io.of(/^\/test-\d+$/);

  // const nps3 = io.of("/test3");
  const userSockets = new Map(); // socketId -> username

  // //Namespaces
  // nps1.on("connection", (socket) => {
  //   console.log(`[io] connected ${socket.id} to nps1`);
  //   socket.on("disconnect", () => {
  //     console.log("User disconnected from /test-1");
  //   });
  //   socket.emit("welcome", "Welcome to the test-1 namespace!");

  //   socket.on("chatMessage", (content) => {
  //     console.log("Chat message:", content);
  //     // Broadcast to everyone in /chat
  //     nps1.emit("chatMessage", { content });
  //   });
  // });

  dynamicNsp.on("connection", async (socketNsp2) => {
    const namespace = socketNsp2.nsp;
    const nsName = namespace.name; // e.g. "/test-2"
    // if (cluster.worker.id === 1) {
    //   // Only first worker logs
    console.log(`[ws] connected ${socketNsp2.id} on ${nsName}`);
    // }

    let currentUsername = null;
    socketNsp2.on("login", async (userName, callback) => {
      console.log(`user ${userName} login`);
      currentUsername = userName;
      userSockets.set(socketNsp2.id, userName);

      socketNsp2.join(userName);

      await db.run("INSERT OR IGNORE INTO users (username) VALUES (?)", [
        userName,
      ]);
      // if (cluster.worker.id === 1) {
      //   // Only first worker logs
      //   console.log(`[io] connected ${userName} to nps2`);
      // }
      namespace.emit("welcome", "Welcome to the test-2 panel!");
      // await db.exec("DROP TABLE IF EXISTS messages;");

      const filtredMsg = await db.all(
        `SELECT id, content, namespace, targetName, senderName
       FROM messages
       WHERE namespace = ?
       AND (targetName = ? OR senderName = ? OR targetName IS NULL)
       ORDER BY id ASC`,
        [nsName, userName, userName]
      );
      // if (cluster.worker.id === 1) {
      //   // Only first worker logs
      //   console.log("get message filtredMsg: " + filtredMsg);
      // }
      const lastID =
        filtredMsg.length > 0 ? filtredMsg[filtredMsg.length - 1].id : null;
      // if (cluster.worker.id === 1) {
      //   // Only first worker logs
      //   console.log("✅ nsp2 lastID respond:", lastID);
      // }
      socketNsp2.emit("get msg", filtredMsg, lastID);
      //  prevent crash on other workers
      if (typeof callback === "function") callback("ok");
    });

    // socketNsp2.on("alert", (data) => {
    //   console.log("test-2 alert:", data);
    //   nps2.emit("alert", `test-2 says: ${data}`);
    // });

    socketNsp2.on("disconnect", () => {
      console.log(`user ${currentUsername} disconnected from /test-2`);
      userSockets.delete(socketNsp2.id);
      // callback("ok");
    });

    socketNsp2.on("user typing", (username, targetName, callback) => {
      console.log("Key pressed typing : ", username);

      if (targetName) {
        namespace.to(targetName).emit("user typing", username);
      } else {
        socketNsp2.broadcast.emit("user typing", username);
      }
      callback("ok");
    });
    // socketNsp2.on("stop typing", (username, callback) => {
    //   console.log("close typing : ", username);
    //   socketNsp2.broadcast.emit("stop typing", username);
    //   callback("ok");
    // });
    socketNsp2.on(
      "chat message nsp2",
      async (msg, targetName, clientOffset, username, callback) => {
        // if (cluster.worker.id === 1) {
        //   // Only first worker logs
        //   console.log("✅ nsp2 msg respond:", {
        //     msg,
        //     targetName,
        //     clientOffset,
        //     namespace: nsName,
        //   });
        // }
        const senderName = userSockets.get(socketNsp2.id) || username;
        console.log("message front: " + JSON.stringify(senderName));

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
          result = await insertMessageStmt.run(
            msg,
            clientOffset,
            targetName,
            senderName,
            nsName
          );
          callback("got it");

          console.log("🚀 ~ result:", result);
        } catch (e) {
          if (e.errno === 19 /* SQLITE_CONSTRAINT */) {
            // console.log("duplicate message, skipping");
          }
          // the message was already inserted, so we notify the client
          callback("got it");
          return;
        }
        const lastMsg = {
          id: result.lastID,
          content: msg,
          targetName,
          senderName,
          namespace: nsName,
        };
        if (targetName) {
          //Join room with targetName to see the msg from the sender
          console.log(`sending msg to ${targetName} from ${senderName}`);

          namespace
            .to([targetName, senderName])
            .emit("chat message nsp2", lastMsg, lastMsg.id);
          // if (targetName !== senderName) {
          //   namespace.emit("chat message nsp2", lastMsg, lastMsg.id);
          // }
          // io.except(userId).emit("chat message", msg);
          // socket.leave(targetName);
          callback("got it");
        } else {
          // Broadcast message: send to everyone
          namespace.emit("chat message nsp2", lastMsg, lastMsg.id);
          callback("got it");
        }
        ///Client delivery
        if (!socketNsp2.recovered) {
          // if the connection state recovery was not successful
          try {
            await db.each(
              "SELECT id, content, targetName, senderName, namespace FROM messages WHERE id > ?",
              [nsp2Socket.handshake.auth.serverOffset || 0],
              (_err, row) => {
                const id = row.id;
                const content = row.content;
                const targetName = row.targetName;
                const senderName = row.senderName;
                const namespace = row.namespace;
                const isSender = senderName || username;
                console.log(`sending msg to ${targetName} from ${msg}`);

                socket.emit("chat message nsp2", {
                  id,
                  content,
                  targetName,
                  isSender,
                  namespace,
                });
              }
            );
          } catch (e) {
            // something went wrong
          }
        }
        // prevent crash on other workers
        if (typeof callback === "function") callback("ok");
      }
    );
  });

  app.get("/", (req, res) => {
    res.sendFile(join(__dirname, "login.html"));
  });
  app.get("/test-0", (req, res) => {
    res.sendFile(join(__dirname, "index.html"));
  });
  app.get("/test-1", (req, res) => {
    res.sendFile(join(__dirname, "nsp1.html"));
  });
  app.get("/test-2", (req, res) => {
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
          result = db.run(
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

    if (!socketNsp2.recovered) {
      try {
        const since = Number(socketNsp2.handshake.auth.serverOffset || 0);
        const rows = await db.all(
          `SELECT id, content, targetName, senderName, namespace
         FROM messages
        WHERE id > ?
          AND namespace = ?
          AND (targetName IS NULL OR targetName = ? OR senderName = ?)
        ORDER BY id ASC`,
          [since, nsName, currentUsername, currentUsername]
        );
        for (const row of rows) {
          socketNsp2.emit("chat message nsp2", row, row.id);
        }
      } catch (e) {
        console.log("Clients recovered err");
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

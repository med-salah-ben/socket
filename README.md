# What Socket.IO is

1. **Socket.IO** is a library that enables low-latency, bidirectional and event-based communication between a client and a server.
2. **Socket.IO** supports both **ECMAScript** & **CommonJS**
3. **Socket.IO** connection can be established:
   - **HTTP long-polling**
   - **WebSocket**
   - **WebTransport**
4. **WebSocket** client will not be able to successfully connect to a **Socket.IO** server, and a **Socket.IO** client will not be able to connect to a plain **WebSocket** server either.
5. **Socket.IO** is not recommended for background services in mobile applications. Use a dedicated messaging platform such as **FCM** instead.

---

# Features

## 1. HTTP long-polling fallback

HTTP long-polling is a technique where the server holds a client’s request open until new data is available, then responds and restarts the process.  
It enables near real-time communication over standard HTTP by reducing unnecessary polling.  
In HTTP long-polling, the server delays its response until it has new data, creating an efficient, continuous data flow over standard HTTP without excessive polling.

---

## 2. Automatic reconnection

In some cases, a WebSocket connection may break silently.  
Socket.IO addresses this with periodic heartbeat checks and automatic reconnection with exponential backoff to maintain a stable connection.

---

## 3. Packet buffering

Packets are temporarily stored while offline and delivered after the client reconnects.

With  **connected attribute** if (socket.connected) OR **volatile events** socket.volatile.emit.

**Example: setInterval() COUNT**

connect / 1 / 2 / **the server is restarted, the client automatically reconnects** / connect / 10 / 11

---

## 4. Acknowledgements

Socket.IO provides a convenient way to send an event and receive a response:  
Add a callback as the last argument of the `emit()`:
//if no callback func, the client never receives an “OK” from the server, so Socket.IO retries 3 times, and i get 4 logs and lose performance and time.  
// Sender
callbackFunc : (response) => ...
// Receiver
callback : callback("got it");


---

## 5. Broadcasting Server Only

Rooms: is an arbitrary channel that sockets can join and leave. It can be used to broadcast events to a subset of connected clients
On the server-side, you can send an event to all connected clients or to a subset of clients: All all connected clients io.emit / connected clients in the "news" room io.to("RoomName").emit
.to() method only exists on the server-side io or socket instance, not on the client-side socket.
// Emit to all connected clients
io.emit("eventName", data);

// Emit to all clients in the "RoomName" room
io.to("RoomName").emit("eventName", data);

---

## 6. Multiplexing
**dynamic namespaces**  
Socket.IO namespaces enable logical separation of features over one connection, such as defining an ‘admin’ area for authorized users.
Scalable: Automatically handles new namespaces without code changes
DRY principle: Single handler for multiple similar namespaces
Flexible: Easy to create namespace-specific chat rooms or channels

**Benefits:** 

Single WebSocket connection handles multiple channels  
Isolated event handlers per namespace  
Better organization of application logic  
Reduced overhead compared to multiple connections  

Creates namespaces matching the pattern /test-0, /test-1, /test-2, etc.
Uses a regular expression to match any namespace starting with /test- followed by digits
All matching namespaces share the same connection handler logic

namespace.emit("chat message nsp2", messageData);
// This sends events TO all clients in the namespace
```

**What it does** 
- Broadcasts events TO multiple clients (or all clients)
- Sends data from server to clients
- It's like making an announcement to EVERYONE

## Visual Comparison
```
┌─────────────────────────────────────────────────────────┐
│                    SERVER                                │
│                                                          │
│  socketNsp2.on("message")  ←── Listens to ONE client   │
│         ↓                                                │
│    Process message                                       │
│         ↓                                                │
│  namespace.emit("message") ──→ Sends to ALL clients     │
│                                                          │
└─────────────────────────────────────────────────────────┘


---

## 7. Catch-All Listeners
socket.**onAny**((eventName, ...args) => This fires for every event received from the client.
socket.**onAnyOutgoing**((eventName, ...args) =>  This fires for every event sent to the client.
socket.**onAny**((eventName, ...args) => {
  console.log("Received:", eventName, args);
});

socket.**onAnyOutgoing**((eventName, ...args) => {
  console.log("Sent:", eventName, args);
});

---

## 8. Handling Disconnections
Socket.IO client is not always connected.   
Socket.IO server does not store any event.  

**connectionStateRecovery** This feature will temporarily store all the events that are sent by the server and will try to restore the state of a client when it reconnects. 

**Server delivery :** Ensures messages from the server reach clients reliably, replaying any missed messages after a disconnect.   

**Client Delivery:** Ensures messages sent by the client reliably reach the server, even after temporary disconnects.   

**serverOffset:** The first serverOffset value like "Pemwzh0.j" is an internal session recovery ID generated by Socket.IO, used to identify and restore the client’s session before your custom message offsets take over.    

**handshake:** The handshake in Socket.IO is the initial connection exchange where the client sends authentication data (like auth.serverOffset) and the server sets up the session.   

---

## 9. Scaling horizontally With @socket.io/cluster-adapter package
**Scaling horizontally:** running multiple Socket.IO server instances behind a load balancer to handle more clients concurrently.  

 By default, Node.js runs your Javascript code in a single thread, which means that even with a 32-core CPU, only one core will be used. Fortunately, the Node.js cluster module provides a convenient way to create one worker thread per core.

There are currently **5 official adapter** implementations:  
the Redis adapter | the Redis Streams adapter | the MongoDB adapter |the Postgres adapter | the Cluster adapter  

Clustering breaks Socket.IO connection state recovery.  
socket.recovered will almost always be false across multiple workers.
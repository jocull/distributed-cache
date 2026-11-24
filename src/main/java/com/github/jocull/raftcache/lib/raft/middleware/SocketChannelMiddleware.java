package com.github.jocull.raftcache.lib.raft.middleware;

import com.github.jocull.raftcache.lib.raft.RaftNode;
import com.github.jocull.raftcache.lib.topology.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A real-socket {@link ChannelMiddleware} (phase 3, see {@code NETWORKING.md} 2.2): messages
 * cross a {@link ServerSocket}/{@link Socket} on {@code 127.0.0.1} using length-prefixed frames
 * of JDK-serialized payloads, satisfying the exact same {@link ChannelSide} contract as
 * {@link PassThruMiddleware} so anything running over PassThru runs unchanged over sockets
 * (invariant N1).
 *
 * <p>One middleware hosts one logical node (the registry is the same {@code addNode} contract
 * as {@link PassThruMiddleware}), so the IT harness builds one middleware per cluster node.
 * The bound {@code host:port} (ephemeral) is the node's wire address, exposed via
 * {@link #getAddress()}; a remote node dials this address through {@link #openChannel}.
 *
 * <p>Handshake (N1): {@code openChannel} dials, writes a {@link Handshake} frame
 * (client + target node addresses), and blocks until it reads back an {@link Acknowledgement}.
 * The server handler reads the handshake, resolves the target node from its registry, and joins
 * {@code target.acceptConnection(side)} — installing the peer's receiver — <em>before</em>
 * writing the ACK. When {@code openChannel} returns, both receivers exist, exactly the
 * guarantee {@code PassThruMiddleware.openChannel}'s {@code .join()} gives.
 *
 * <p>Fault injection (N3, PassThru parity): {@link #interrupt(NodeAddress)} seals a logical
 * node: its outbound sides drop {@code send()} at the door, and its inbound sides keep consuming
 * frames from the socket but discard the deserialized payload — the byte stream never
 * desynchronizes (reader discipline N3). {@link #restore(NodeAddress)} ends the sealing and
 * traffic resumes in order. Neither knob touches the wire framing itself.
 *
 * <p>Lifecycle: a dead socket marks the channel dead (later sends are dropped and logged, reads
 * end); re-establishment is the node's own {@code connectWithTopology}/{@code connectTo} paths
 * (auto-reconnect policy is a recorded follow-up, {@code NETWORKING.md} 6.1).
 * {@link #close()} is idempotent and tears down the server socket plus every channel; all
 * threads are daemons so a JVM can always exit.
 *
 * <p><b>Bootstrap discipline (same as PassThru).</b> {@code openChannel} blocks its caller
 * (typically a node's single-threaded executor) until the peer's {@code acceptConnection}
 * completes, so two nodes must not dial each other from their executors <em>simultaneously</em> —
 * the existing test ritual of running each node's {@code connectWithTopology} sequentially is
 * required, exactly as with {@code PassThruMiddleware}.
 */
public class SocketChannelMiddleware implements ChannelMiddleware, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketChannelMiddleware.class);
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();

    private final ServerSocket serverSocket;
    /** The bound host:port — this middleware's (its node's) wire address. */
    private final NodeAddress ownAddress;
    private final Thread acceptorThread;
    /** Registry keyed by node address, same contract as {@link PassThruMiddleware#addNode}. */
    private final Map<NodeAddress, RaftNode> addressRaftNodeMap = new HashMap<>();
    /** Every connection this middleware participates in, on either side. */
    private final Set<Connection> connections = ConcurrentHashMap.newKeySet();
    /** Logical nodes currently sealed (partitioned). Checked on every send/deliver of every side. */
    private final Set<NodeAddress> interruptedNodes = ConcurrentHashMap.newKeySet();
    private volatile boolean closed = false;

    /**
     * Bind an ephemeral port on {@code 127.0.0.1}. The bound address is the wire address this
     * middleware's node is reachable at; other nodes dial it with {@link #openChannel}.
     */
    public SocketChannelMiddleware() {
        this("127.0.0.1");
    }

    /**
     * Bind an ephemeral port on {@code host}.
     *
     * @throws IllegalStateException if the socket cannot be bound
     */
    public SocketChannelMiddleware(String host) {
        Objects.requireNonNull(host, "host");
        final ServerSocket bound;
        try {
            bound = new ServerSocket();
            bound.setReuseAddress(true);
            bound.bind(new InetSocketAddress(host, 0));
        } catch (IOException e) {
            throw new IllegalStateException("Could not bind server socket on " + host, e);
        }
        this.serverSocket = bound;
        this.ownAddress = new NodeAddress(host + ":" + bound.getLocalPort());

        final Thread acceptor = new Thread(this::acceptLoop,
                "socket-mw-acceptor-" + THREAD_COUNTER.getAndIncrement());
        acceptor.setDaemon(true);
        acceptor.start();
        this.acceptorThread = acceptor;
    }

    /**
     * @return this middleware's bound wire address (the address remote nodes should dial).
     */
    public NodeAddress getAddress() {
        return ownAddress;
    }

    public SocketChannelMiddleware addNode(RaftNode raftNode) {
        Objects.requireNonNull(raftNode, "raftNode must not be null");
        synchronized (addressRaftNodeMap) {
            addressRaftNodeMap.compute(raftNode.getNodeAddress(), (k, v) -> {
                if (v != null) {
                    throw new IllegalArgumentException("Node " + v.getId() + " already registered!");
                }
                return raftNode;
            });
        }
        return this;
    }

    public boolean removeNode(RaftNode raftNode) {
        synchronized (addressRaftNodeMap) {
            return addressRaftNodeMap.remove(raftNode.getNodeAddress()) != null;
        }
    }

    public Map<NodeAddress, RaftNode> getRegisteredNodes() {
        synchronized (addressRaftNodeMap) {
            return Map.copyOf(addressRaftNodeMap);
        }
    }

    // ---------------------------------------------------------------------------
    // Fault injection — PassThru parity over sockets (NETWORKING.md 2.2/3, invariant N3)
    // ---------------------------------------------------------------------------

    /**
     * Seal the logical node: its outbound sends drop at the door and its inbound frames are
     * consumed but discarded, so the wire stays framed and the far side never desynchronizes.
     * Mirrors {@link PassThruMiddleware#interrupt}.
     */
    public void interrupt(NodeAddress nodeAddress) {
        Objects.requireNonNull(nodeAddress, "nodeAddress must not be null");
        if (interruptedNodes.add(nodeAddress)) {
            LOGGER.info("Socket network: interrupting node {}", nodeAddress.getAddress());
        }
    }

    /** End a partition started by {@link #interrupt(NodeAddress)}; traffic resumes in order. */
    public void restore(NodeAddress nodeAddress) {
        Objects.requireNonNull(nodeAddress, "nodeAddress must not be null");
        if (interruptedNodes.remove(nodeAddress)) {
            LOGGER.info("Socket network: restoring node {}", nodeAddress.getAddress());
        }
    }

    public boolean isInterrupted(NodeAddress nodeAddress) {
        return nodeAddress != null && interruptedNodes.contains(nodeAddress);
    }

    // ---------------------------------------------------------------------------
    // ChannelMiddleware
    // ---------------------------------------------------------------------------

    @Override
    public ChannelSide openChannel(RaftNode source, NodeAddress targetAddress) {
        Objects.requireNonNull(source, "source must not be null");
        final NodeAddress clientAddress = Objects.requireNonNull(source.getNodeAddress(),
                "source.getNodeAddress() must not be null");
        if (closed) {
            throw new IllegalStateException("This middleware is closed");
        }

        final int sep = targetAddress.getAddress().lastIndexOf(':');
        if (sep < 0) {
            throw new IllegalArgumentException("Expected host:port node address, got " + targetAddress);
        }
        final String host = targetAddress.getAddress().substring(0, sep);
        final int port;
        try {
            port = Integer.parseInt(targetAddress.getAddress().substring(sep + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expected host:port node address, got " + targetAddress);
        }

        final Socket socket;
        try {
            socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
        } catch (IOException e) {
            throw new IllegalStateException("Could not connect to " + targetAddress, e);
        }

        final Connection connection = new Connection(socket, clientAddress, targetAddress);
        connections.add(connection);
        try {
            connection.sendLocked(new Handshake(clientAddress, targetAddress));
            final Object ack = connection.readFrame();
            if (!(ack instanceof Acknowledgement)) {
                throw new IllegalStateException(
                        "Expected Acknowledgement from " + targetAddress + ", got " + ack);
            }
        } catch (IOException e) {
            connections.remove(connection);
            connection.closeQuietly();
            throw new IllegalStateException("Handshake with " + targetAddress + " failed", e);
        }
        connection.startReader();
        LOGGER.debug("Channel open {} <-> {} (client side)", clientAddress.getAddress(),
                targetAddress.getAddress());
        return new SocketChannelSide(connection);
    }

    /**
     * Close the server socket and every connection. Idempotent: subsequent calls do nothing and
     * {@link #openChannel} starts failing immediately.
     */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.debug("Error closing server socket: {}", e.toString());
        }
        for (Connection connection : connections) {
            connection.closeQuietly();
        }
    }

    // ---------------------------------------------------------------------------
    // Acceptor
    // ---------------------------------------------------------------------------

    private void acceptLoop() {
        while (!closed) {
            final Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                if (closed) {
                    break;
                }
                LOGGER.debug("Accept failed: {}", e.toString());
                continue;
            }
            final Thread handler = new Thread(() -> handleIncoming(socket),
                    "socket-mw-handler-" + THREAD_COUNTER.getAndIncrement());
            handler.setDaemon(true);
            handler.start();
        }
    }

    /**
     * Handoff sequence for one incoming connection: read the handshake, resolve the target
     * node from the registry, join {@code acceptConnection} (receiver installed), write the
     * ACK — and then this thread becomes the connection's reader loop.
     */
    private void handleIncoming(Socket socket) {
        try {
            socket.setTcpNoDelay(true);
        } catch (IOException e) {
            LOGGER.debug("Could not set TCP_NODELAY: {}", e.toString());
        }
        Connection connection = null;
        try {
            final Object first = readFirstFrame(socket);
            final Handshake handshake;
            try {
                handshake = (Handshake) first;
            } catch (ClassCastException e) {
                throw new IllegalStateException("Expected Handshake frame, got " + first, e);
            }
            final RaftNode target;
            synchronized (addressRaftNodeMap) {
                target = addressRaftNodeMap.get(handshake.getTarget());
            }
            if (target == null) {
                LOGGER.debug("Handshake from {} addressed to unknown node {} — closing connection",
                        handshake.getClient().getAddress(), handshake.getTarget().getAddress());
                socket.close();
                return;
            }
            connection = new Connection(socket, target.getNodeAddress(), handshake.getClient());
            connections.add(connection);
            final SocketChannelSide side = new SocketChannelSide(connection);
            LOGGER.debug("Accepted connection: client {} -> node {}",
                    handshake.getClient().getAddress(), target.getId());
            // N1: the peer's receiver is installed before the ACK is written, so when
            // openChannel returns on the far side both receiver sinks already exist.
            target.acceptConnection(side).join();
            connection.sendLocked(new Acknowledgement());
        } catch (EOFException e) {
            LOGGER.debug("Client closed the connection during the handshake");
        } catch (Exception e) {
            LOGGER.debug("Handshake handling failed", e);
        }
        if (connection != null) {
            // This handler thread is now the connection's reader.
            connection.runReaderLoop();
        } else {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }

    // Reads the first (handshake) frame directly off the socket, before a Connection exists.
    private static Object readFirstFrame(Socket socket) throws IOException {
        final DataInputStream in = new DataInputStream(socket.getInputStream());
        final int length = in.readInt();
        if (length < 0 || length > 64 * 1024 * 1024) {
            throw new EOFException("Implausible frame length " + length);
        }
        final byte[] payload = new byte[length];
        in.readFully(payload);
        return deserialize(payload);
    }

    // ---------------------------------------------------------------------------
    // Connection + side
    // ---------------------------------------------------------------------------

    /**
     * One socket endpoint: the streams, the (single) reader, the send lock, and the
     * receiver-buffering state shared with the {@link SocketChannelSide} this middleware
     * exposes for it.
     *
     * <p>Threading: exactly one reader (this thread or the handler thread); sends serialized
     * under {@link #sendLock}; receiver state under {@link #receiverLock}. Delivery is strictly
     * FIFO per channel — stronger than PassThru's reordered hops, and order-tolerant above
     * (Raft's term/index guards already handle late arrivals).
     */
    private final class Connection {
        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;
        private final Object sendLock = new Object();
        /** The logical node whose side this is (sealing key — matches PassThru's per-node isolation). */
        private final NodeAddress ownAddress;
        /** The peer's address — what {@link ChannelSide#getAddress()} reports (the must-not-change contract). */
        private final NodeAddress remoteAddress;

        private final Object receiverLock = new Object();
        private Consumer<Object> receiver;
        private final List<Object> buffered = new ArrayList<>();
        private volatile boolean dead = false;

        private Connection(Socket socket, NodeAddress ownAddress, NodeAddress remoteAddress) {
            this.socket = socket;
            this.ownAddress = ownAddress;
            this.remoteAddress = remoteAddress;
            try {
                this.in = new DataInputStream(socket.getInputStream());
                this.out = new DataOutputStream(socket.getOutputStream());
            } catch (IOException e) {
                // A live socket must expose its streams; failure means the socket is unusable.
                throw new IllegalStateException("Socket to " + remoteAddress + " is unusable", e);
            }
        }

        private boolean isDown() {
            return !closed && isInterrupted(ownAddress);
        }

        /**
         * Deliver to the receiver, buffered until {@code setReceiver} (belt-and-braces: the
         * handshake order already makes an uninstalled receiver impossible, the buffer keeps
         * the side self-containedly safe). Sealed nodes drop the message — the frame has
         * already been consumed, so the stream stays framed (N3).
         */
        private void deliver(Object message) {
            if (isDown()) {
                LOGGER.debug("Dropping inbound {} from {} (node {} is sealed)",
                        message.getClass().getSimpleName(), remoteAddress, ownAddress);
                return;
            }
            final Consumer<Object> sink;
            synchronized (receiverLock) {
                if (receiver == null) {
                    buffered.add(message);
                    return;
                }
                sink = receiver;
            }
            sink.accept(message); // outside the lock: the receiver dispatches onto the node executor
        }

        private void setReceiver(Consumer<Object> newReceiver) {
            Objects.requireNonNull(newReceiver, "newReceiver must not be null");
            final List<Object> backlog;
            synchronized (receiverLock) {
                this.receiver = newReceiver;
                if (buffered.isEmpty()) {
                    return;
                }
                backlog = new ArrayList<>(buffered);
                buffered.clear();
            }
            for (Object message : backlog) {
                deliver(message);
            }
        }

        private void send(Object message) {
            if (dead || isDown()) {
                if (!dead) {
                    LOGGER.debug("Dropping send {} to {} (node {} is sealed)",
                            message.getClass().getSimpleName(), remoteAddress, ownAddress);
                }
                return;
            }
            synchronized (sendLock) {
                if (dead) {
                    return;
                }
                try {
                    writeFrame(out, message);
                } catch (IOException e) {
                    LOGGER.debug("Send to {} failed — channel is dead", remoteAddress, e);
                    dead = true;
                }
            }
        }

        /** Handshake-phase write; no sendLock/Dead check because no concurrent readers exist yet. */
        private void sendLocked(Object message) throws IOException {
            writeFrame(out, message);
        }

        private Object readFrame() throws IOException {
            final int length = in.readInt();
            if (length < 0 || length > 64 * 1024 * 1024) {
                throw new EOFException("Implausible frame length " + length);
            }
            final byte[] payload = new byte[length];
            in.readFully(payload);
            return deserialize(payload);
        }

        private void startReader() {
            final Thread reader = new Thread(this::runReaderLoop,
                    "socket-mw-reader-" + THREAD_COUNTER.getAndIncrement());
            reader.setDaemon(true);
            reader.start();
        }

        private void runReaderLoop() {
            try {
                while (true) {
                    final Object message = readFrame();
                    deliver(message);
                }
            } catch (EOFException normal) {
                LOGGER.debug("Connection to {} closed by the peer", remoteAddress);
            } catch (SocketException expected) {
                // close() interrupted the read in progress — shutdown, not a failure.
                LOGGER.debug("Reader on channel to {} stopped (socket closed)", remoteAddress);
            } catch (Throwable t) {
                // A delivery that cannot run (e.g. the peer node's executor is shutting down)
                // leaves the channel unusable; dead-mark and drop the socket.
                LOGGER.debug("Reader on channel to {} stopped", remoteAddress, t);
            } finally {
                closeQuietly();
            }
        }

        private void closeQuietly() {
            dead = true;
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }

    private final class SocketChannelSide implements ChannelSide {
        private final Connection connection;

        private SocketChannelSide(Connection connection) {
            this.connection = connection;
        }

        @Override
        public NodeAddress getAddress() {
            return connection.remoteAddress;
        }

        @Override
        public void send(Object message) {
            connection.send(message);
        }

        @Override
        public void setReceiver(Consumer<Object> receiver) {
            connection.setReceiver(receiver);
        }

        /**
         * Closes the underlying socket (dropping it from this middleware's registry). Closing
         * the socket also ends the peer side's reader (EOF / SocketException), so the link dies
         * both ways; see {@link Connection#closeQuietly()}. Idempotent.
         */
        @Override
        public void close() {
            connections.remove(connection);
            connection.closeQuietly();
        }
    }

    // ---------------------------------------------------------------------------
    // Frame codec — 4-byte BE length + one JDK-serialized payload per frame (NETWORKING.md 2.1)
    // ---------------------------------------------------------------------------

    private static byte[] serialize(Object payload) throws IOException {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(payload);
        }
        return bos.toByteArray();
    }

    private static Object deserialize(byte[] payload) throws IOException {
        try {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
                return ois.readObject();
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Wire payload of unknown class: " + e.getMessage(), e);
        }
    }

    private static void writeFrame(DataOutputStream out, Object payload) throws IOException {
        final byte[] bytes = serialize(payload);
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }
}

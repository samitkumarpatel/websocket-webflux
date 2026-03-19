package net.samitkumar.websocket_webflux;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

// ════════════════════════════════════════════════════════════════════════════
//  TOPOLOGY OVERVIEW
//
//  Per USER (permanent, created once):
//    • user.{id}.ex      — fanout exchange, durable
//                          ↑ This changed from "direct" to "fanout" so that
//                            publishing once fans out to ALL active sessions.
//
//  Per SESSION (per WS connection, auto-cleaned):
//    • user.{id}.session.{uuid}  — non-durable, auto-delete queue
//                                  bound to user.{id}.ex on connect
//                                  deleted automatically on disconnect
//
//  Per GROUP (permanent, created once):
//    • group.{id}.ex     — fanout exchange, durable
//                          binds to user.{id}.ex (exchange-to-exchange binding)
//                          NOT to individual session queues
//
//  Message flow:
//    DM send         → publish to user.{recipient}.ex
//                      → fans out to all recipient's session queues
//                      → delivered to every open tab
//
//    Group send      → publish to group.{id}.ex
//                      → E2E bindings fan out to each member's user.{id}.ex
//                      → each user.ex fans out to all their session queues
//                      → every open tab of every member receives it
//
//    addMember       → bind group.{id}.ex  →  user.{id}.ex   (E2E binding)
//    removeMember    → unbind that E2E binding
//    WS connect      → create session queue, bind to user.{id}.ex
//    WS disconnect   → session queue auto-deletes (auto-delete flag)
// ════════════════════════════════════════════════════════════════════════════

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class WebsocketWebfluxApplication {
    private final MessagingService messagingService;
    private final GroupService groupService;
    private final JsonMapper jsonMapper;

    public static void main(String[] args) {
        SpringApplication.run(WebsocketWebfluxApplication.class, args);
    }

    @Bean
    RouterFunction<ServerResponse> routes(ChatRestHandler rest) {
        return RouterFunctions.route()
                // User provisioning
                .POST("/api/users", rest::provisionUser)
                // Group lifecycle
                .POST("/api/groups", rest::createGroup)
                .POST("/api/groups/{gid}/members", rest::addMember)
                .DELETE("/api/groups/{gid}/members/{uid}", rest::removeMember)
                .GET("/api/groups", rest::listGroupsForUser)
                // History
                .GET("/api/history/dm/{a}/{b}", rest::dmHistory)
                .GET("/api/history/group/{gid}", rest::groupHistory)
                .build();
    }

    @Bean
    public HandlerMapping handlerMapping() {
        Map<String, WebSocketHandler> map = new HashMap<>();

        map.put("/ws/chat", session -> {

            String userId = param(session, "userId");
            if (userId == null || userId.isBlank()) return session.close();

            log.info("WS connect: userId={} sessionId={}", userId, session.getId());

            // 1. Ensure user's fanout exchange exists (idempotent)
            messagingService.provisionUser(userId);

            // 2. Create a fresh session queue for THIS connection and bind it
            //    to the user's exchange. Auto-delete means RabbitMQ cleans it
            //    up automatically when this WebSocket closes — no manual cleanup.
            String sessionQueue = messagingService.createSessionQueue(userId, session.getId());

            // 3. Consume from the session queue — receives DMs and all group messages
            Flux<String> outbound = messagingService.subscribe(sessionQueue)
                    .flatMap(msg -> Mono.fromCallable(() -> jsonMapper.writeValueAsString(msg)))
                    .onErrorContinue((err, o) ->
                            log.warn("Outbound error for {}: {}", userId, err.getMessage()));

            // 4. Handle commands from the client
            Mono<Void> inbound = session.receive()
                    .flatMap(wsMsg -> handleCommand(userId, wsMsg.getPayloadAsText()))
                    .then();

            return Mono.zip(session.send(outbound.map(session::textMessage)), inbound)
                    .doFinally(sig -> log.info("WS disconnect: userId={} sessionId={} signal={}", userId, session.getId(), sig))
                    .then();

        });
        return new SimpleUrlHandlerMapping(map, -1);
    }

    private Mono<Void> handleCommand(String userId, String json) {
        try {
            JsonNode node = jsonMapper.readTree(json);
            String type = node.path("type").asText();
            String to = node.path("to").asText();
            String content = node.path("content").asText();

            if (content.isBlank() || to.isBlank()) return Mono.empty();

            return switch (type) {

                case "SEND_DM" -> {
                    // Ensure recipient is provisioned before sending
                    messagingService.provisionUser(to);
                    yield messagingService.sendDm(userId, to, content).then();
                }

                case "SEND_GROUP" -> groupService.findById(to)
                        .flatMap(group -> {
                            if (!group.hasMember(userId)) {
                                log.warn("User {} is not a member of group {}", userId, to);
                                return Mono.empty();
                            }
                            return messagingService.sendGroup(userId, to, content).then();
                        });

                default -> {
                    log.debug("Unknown command '{}' from {}", type, userId);
                    yield Mono.empty();
                }
            };

        } catch (Exception e) {
            log.warn("Bad command from {}: {}", userId, e.getMessage());
            return Mono.empty();
        }
    }

    private String param(WebSocketSession s, String key) {
        String q = s.getHandshakeInfo().getUri().getQuery();
        if (q == null) return null;
        for (String p : q.split("&"))
            if (p.startsWith(key + "=")) return p.substring(key.length() + 1);
        return null;
    }

    @Bean
    WebSocketHandlerAdapter webSocketHandlerAdapter() {
        HandshakeWebSocketService handshakeWebSocketService = new HandshakeWebSocketService() {
            @Override
            public Mono<Void> handleRequest(ServerWebExchange exchange, WebSocketHandler handler) {
                // Wrap the handler to intercept lifecycle
                WebSocketHandler decoratedHandler = session -> {
                    String sessionId = session.getId();

                    return handler.handle(session)
                            .doOnSubscribe(sub -> {
                                session.getAttributes().put("a_id", UUID.randomUUID());
                                log.info("HANDSHAKE - Session JOINED: sessionId={}", sessionId);
                            })
                            .doFinally(signal -> {
                                log.info("HANDSHAKE - Session LEFT: sessionId={}, reason={}", sessionId, signal);
                            });
                };
                return super.handleRequest(exchange, decoratedHandler);
            }
        };
        return new WebSocketHandlerAdapter(handshakeWebSocketService);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RABBIT TOPOLOGY — the whole model in constants and two helper methods
//
//  Every user gets:
//    • user.{id}.ex     — direct exchange (their personal DM address)
//    • user.{id}.queue  — durable queue   (the single permanent mailbox)
//
//  Every group gets:
//    • group.{id}.ex    — fanout exchange
//
//  addMember   → binds   user.{id}.queue  to  group.{id}.ex
//  removeMember→ unbinds user.{id}.queue  from group.{id}.ex
//
//  DM send     → publish to user.{recipient}.ex  (routed to their queue)
//  Group send  → publish to group.{id}.ex        (routed to all bound queues)
// ─────────────────────────────────────────────────────────────────────────────

@Configuration
class RabbitConfig {

    static String userExchange(String userId) {
        return "user." + userId + ".ex";
    }

    static String sessionQueue(String userId, String sid) {
        return "user." + userId + ".session." + sid;
    }

    static String groupExchange(String gid) {
        return "group." + gid + ".ex";
    }

    @Bean
    ConnectionFactory reactorCf(org.springframework.amqp.rabbit.connection.CachingConnectionFactory cachingConnectionFactory) {
        return cachingConnectionFactory.getRabbitConnectionFactory();
    }

    @Bean
    Sender sender(ConnectionFactory cf) {
        return RabbitFlux.createSender(new SenderOptions().connectionFactory(cf));
    }

    @Bean
    Receiver receiver(ConnectionFactory cf) {
        return RabbitFlux.createReceiver(new ReceiverOptions().connectionFactory(cf));
    }

    @Bean
    RabbitAdmin rabbitAdmin(org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory) {
        return new RabbitAdmin(connectionFactory);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MODELS
// ─────────────────────────────────────────────────────────────────────────────

@Data
@Document(collection = "chat_messages")
@CompoundIndexes({
        @CompoundIndex(name = "dm_idx", def = "{'senderId':1,'recipientId':1,'sentAt':-1}"),
        @CompoundIndex(name = "group_idx", def = "{'groupId':1,'sentAt':-1}")
})
class ChatMessage {
    @Id
    String id;
    MessageType type;   // DM | GROUP
    String senderId;
    String recipientId; // DM only
    String groupId;     // GROUP only
    String content;
    Instant sentAt = Instant.now();

    enum MessageType {DM, GROUP}
}

@Data
@Document(collection = "chat_groups")
class ChatGroup {
    @Id
    String id;
    String name;
    String createdBy;
    List<String> memberIds = new ArrayList<>();
    Instant createdAt = Instant.now();

    boolean hasMember(String uid) {
        return memberIds.contains(uid);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// REPOSITORIES
// ─────────────────────────────────────────────────────────────────────────────

interface MessageRepository extends ReactiveMongoRepository<ChatMessage, String> {

    @Query("{ $or: [{ senderId:?0, recipientId:?1 }, { senderId:?1, recipientId:?0 }] }")
    Flux<ChatMessage> findDmHistory(String a, String b, org.springframework.data.domain.Pageable p);

    Flux<ChatMessage> findByGroupIdOrderBySentAtDesc(String gid, org.springframework.data.domain.Pageable p);
}

interface GroupRepository extends ReactiveMongoRepository<ChatGroup, String> {
    Flux<ChatGroup> findByMemberIdsContaining(String userId);
}

// ─────────────────────────────────────────────────────────────────────────────
// MESSAGING SERVICE
//
// Core rules implemented here:
//   provisionUser  → declare user exchange + user queue + bind queue to exchange
//   createGroup    → declare group fanout exchange
//   addMember      → bind  user.queue → group.exchange   (one line!)
//   removeMember   → unbind user.queue from group.exchange (one line!)
//   sendDm         → publish to recipient's exchange
//   sendGroup      → publish to group's exchange
//   subscribe      → consume from user's OWN permanent queue (not a temp one)
// ─────────────────────────────────────────────────────────────────────────────

@Slf4j
@Service
@RequiredArgsConstructor
class MessagingService {

    private final Sender sender;
    private final Receiver receiver;
    private final RabbitAdmin rabbitAdmin;
    private final JsonMapper jsonMapper;
    private final MessageRepository messageRepository;

    // ── User provisioning ─────────────────────────────────────────────────────

    /**
     * Called once per user on first connect (idempotent).
     * <p>
     * Creates ONLY the user's exchange — a durable fanout.
     * No queue is created here. Queues are created per WS session instead.
     * <p>
     * Why fanout (not direct)?
     * Publishing once to a fanout exchange delivers the message to ALL
     * queues bound to it simultaneously. So when Alice has 3 tabs open,
     * all 3 session queues receive the same message — no loop needed.
     */
    void provisionUser(String userId) {
        rabbitAdmin.declareExchange(
                ExchangeBuilder.fanoutExchange(RabbitConfig.userExchange(userId))
                        .durable(true).build()
        );
        log.info("User exchange ready: {}", RabbitConfig.userExchange(userId));
    }

    /**
     * Called on every WebSocket connect. Creates a fresh session queue and
     * binds it to the user's fanout exchange.
     * <p>
     * Properties:
     * • non-durable  — no need to persist the queue itself
     * • auto-delete  — RabbitMQ deletes it the moment the consumer disconnects
     * • exclusive    — only this consumer can use it (optional but clean)
     * <p>
     * Returns the session queue name so the caller can consume from it.
     * <p>
     * Because the queue binds to a fanout exchange, it automatically receives:
     * • DMs addressed to this user
     * • Messages from every group this user is in (via E2E bindings — see below)
     * No extra work needed when new groups are joined — the exchange topology
     * handles it transparently.
     */
    String createSessionQueue(String userId, String sessionId) {
        String userEx = RabbitConfig.userExchange(userId);
        String sessionQ = RabbitConfig.sessionQueue(userId, sessionId);

        rabbitAdmin.declareQueue(
                QueueBuilder.nonDurable(sessionQ).autoDelete()/*.exclusive()*/.build()
        );
        // Bind this session queue to the user's fanout exchange
        rabbitAdmin.declareBinding(
                new Binding(sessionQ, Binding.DestinationType.QUEUE, userEx, "", null)
        );

        log.info("Session queue created: {} → {}", sessionQ, userEx);
        return sessionQ;
    }

    // ── Group lifecycle ───────────────────────────────────────────────────────

    /**
     * Declare the group's fanout exchange (idempotent).
     */
    void provisionGroup(String groupId) {
        rabbitAdmin.declareExchange(
                ExchangeBuilder.fanoutExchange(RabbitConfig.groupExchange(groupId))
                        .durable(true).build()
        );
    }

    /**
     * Add a member to a group.
     * <p>
     * Uses a RabbitMQ Exchange-to-Exchange (E2E) binding:
     * group.{id}.ex  →  user.{uid}.ex
     * <p>
     * This is the key design decision.
     * We bind the GROUP exchange to the USER exchange — not to any session queue.
     * This means:
     * • The binding is permanent — survives session reconnects
     * • Any session queue Alice creates later automatically receives group msgs
     * • Adding a member is one broker operation regardless of how many sessions
     * <p>
     * When Alice sends a group message:
     * group.G1.ex → (E2E) → user.alice.ex, user.bob.ex, user.carol.ex
     * → each user's fanout → all their session queues
     */
    void addMemberToGroup(String groupId, String userId) {
        // Exchange-to-exchange binding: source=group exchange, destination=user exchange
        rabbitAdmin.declareBinding(new Binding(
                RabbitConfig.userExchange(userId),           // destination exchange
                Binding.DestinationType.EXCHANGE,            // ← EXCHANGE, not QUEUE
                RabbitConfig.groupExchange(groupId),         // source exchange
                "",
                null
        ));
        log.info("E2E bound: group.{}.ex → user.{}.ex", groupId, userId);
    }

    /**
     * Remove a member — just unbind the E2E.
     * All their session queues immediately stop receiving group messages.
     */
    void removeMemberFromGroup(String groupId, String userId) {
        rabbitAdmin.removeBinding(new Binding(
                RabbitConfig.userExchange(userId),
                Binding.DestinationType.EXCHANGE,
                RabbitConfig.groupExchange(groupId),
                "",
                null
        ));
        log.info("E2E unbound: group.{}.ex from user.{}.ex", groupId, userId);
    }

    // ── Sending ───────────────────────────────────────────────────────────────

    /**
     * DM: publish to the RECIPIENT's fanout exchange.
     * Fans out to all their active session queues → all open tabs receive it.
     * If recipient has no active session, message is dropped (non-durable sessions).
     * For offline delivery, persist to MongoDB and replay on reconnect.
     */
    Mono<ChatMessage> sendDm(String senderId, String recipientId, String content) {
        var msg = buildMessage(senderId, content, m -> {
            m.setType(ChatMessage.MessageType.DM);
            m.setRecipientId(recipientId);
        });
        return messageRepository.save(msg)
                .flatMap(saved -> publish(RabbitConfig.userExchange(recipientId), saved)
                        .thenReturn(saved));
    }

    /**
     * Group message: publish once to the group fanout exchange.
     * Flow: group.ex → E2E → each member's user.ex → each user's session queues.
     * One publish delivers to N members × M sessions each.
     */
    Mono<ChatMessage> sendGroup(String senderId, String groupId, String content) {
        var msg = buildMessage(senderId, content, m -> {
            m.setType(ChatMessage.MessageType.GROUP);
            m.setGroupId(groupId);
        });
        return messageRepository.save(msg)
                .flatMap(saved -> publish(RabbitConfig.groupExchange(groupId), saved)
                        .thenReturn(saved));
    }

    private ChatMessage buildMessage(String senderId, String content,
                                     java.util.function.Consumer<ChatMessage> mutate) {
        var m = new ChatMessage();
        m.setId(UUID.randomUUID().toString());
        m.setSenderId(senderId);
        m.setContent(content);
        mutate.accept(m);
        return m;
    }

    private Mono<Void> publish(String exchange, ChatMessage msg) {
        return Mono.fromCallable(() -> jsonMapper.writeValueAsBytes(msg))
                .flatMap(body -> {
                    var props = new AMQP.BasicProperties.Builder()
                            .contentType("application/json")
                            .deliveryMode(2) // persistent — survives broker restart
                            .build();
                    return sender.send(Mono.just(new OutboundMessage(exchange, "", props, body)
                    ));
                })
                .doOnError(e -> log.error("Publish failed to {}", exchange, e));
    }

    // ── Subscribing ───────────────────────────────────────────────────────────

    /**
     * Subscribe by consuming from a session queue that was already created
     * by createSessionQueue(). The queue is auto-delete so RabbitMQ cleans
     * it up the moment this consumer (the WebSocket) disconnects.
     */
    Flux<ChatMessage> subscribe(String sessionQueue) {
        return receiver.consumeAutoAck(sessionQueue)
                .mapNotNull(delivery -> {
                    try {
                        return jsonMapper.readValue(
                                new String(delivery.getBody(), StandardCharsets.UTF_8),
                                ChatMessage.class
                        );
                    } catch (Exception e) {
                        log.error("Deserialise error on {}", sessionQueue, e);
                        return null;
                    }
                });
    }

    // ── History ───────────────────────────────────────────────────────────────

    Flux<ChatMessage> dmHistory(String a, String b, int limit) {
        return messageRepository.findDmHistory(a, b,
                PageRequest.of(0, limit, Sort.by("sentAt").descending()));
    }

    Flux<ChatMessage> groupHistory(String gid, int limit) {
        return messageRepository.findByGroupIdOrderBySentAtDesc(gid,
                PageRequest.of(0, limit));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GROUP SERVICE
// ─────────────────────────────────────────────────────────────────────────────

@Slf4j
@Service
@RequiredArgsConstructor
class GroupService {

    private final GroupRepository groupRepository;
    private final MessagingService messagingService;

    Mono<ChatGroup> createGroup(String createdBy, String name, List<String> memberIds) {
        if (!memberIds.contains(createdBy)) memberIds.add(0, createdBy);

        var group = new ChatGroup();
        group.setId(UUID.randomUUID().toString());
        group.setName(name);
        group.setCreatedBy(createdBy);
        group.setMemberIds(new ArrayList<>(memberIds));

        return groupRepository.save(group)
                .doOnSuccess(g -> {
                    // Declare the fanout exchange for this group
                    messagingService.provisionGroup(g.getId());
                    // Bind each initial member's permanent queue to this exchange
                    g.getMemberIds().forEach(uid -> messagingService.addMemberToGroup(g.getId(), uid));
                });
    }

    Mono<ChatGroup> addMember(String groupId, String userId) {
        return groupRepository.findById(groupId)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Group not found")))
                .flatMap(g -> {
                    if (!g.hasMember(userId)) g.getMemberIds().add(userId);
                    return groupRepository.save(g);
                })
                .doOnSuccess(g ->
                        // The only RabbitMQ operation: bind user's existing queue to group exchange
                        messagingService.addMemberToGroup(groupId, userId)
                );
    }

    Mono<ChatGroup> removeMember(String groupId, String userId) {
        return groupRepository.findById(groupId)
                .flatMap(g -> {
                    g.getMemberIds().remove(userId);
                    return groupRepository.save(g);
                })
                .doOnSuccess(g ->
                        // The only RabbitMQ operation: unbind user's queue from group exchange
                        messagingService.removeMemberFromGroup(groupId, userId)
                );
    }

    Mono<ChatGroup> findById(String id) {
        return groupRepository.findById(id);
    }

    Flux<ChatGroup> groupsForUser(String userId) {
        return groupRepository.findByMemberIdsContaining(userId);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// REST HANDLER
// ─────────────────────────────────────────────────────────────────────────────

@Slf4j
@Component
@RequiredArgsConstructor
class ChatRestHandler {

    private final MessagingService messagingService;
    private final GroupService groupService;

    /**
     * POST /api/users  { "userId": "alice" }
     * Provisions the user's exchange + queue on demand (also called on WS connect).
     */
    Mono<ServerResponse> provisionUser(ServerRequest req) {
        return req.bodyToMono(UserProvisionRequest.class)
                .doOnNext(b -> messagingService.provisionUser(b.userId()))
                .flatMap(b -> ServerResponse.ok().bodyValue(
                        Map.of("userId", b.userId(), "status", "provisioned")
                ));
    }

    /**
     * POST /api/groups  { "name": "...", "createdBy": "alice", "memberIds": ["alice","bob"] }
     */
    Mono<ServerResponse> createGroup(ServerRequest req) {
        return req.bodyToMono(CreateGroupRequest.class)
                .flatMap(b -> groupService.createGroup(b.createdBy(), b.name(),
                        new ArrayList<>(b.memberIds())))
                .flatMap(g -> ServerResponse.ok().bodyValue(g));
    }

    /**
     * POST /api/groups/{gid}/members  { "userId": "carol" }
     */
    Mono<ServerResponse> addMember(ServerRequest req) {
        return req.bodyToMono(MemberRequest.class)
                .flatMap(b -> {
                    // Ensure the new member is provisioned first
                    messagingService.provisionUser(b.userId());
                    return groupService.addMember(req.pathVariable("gid"), b.userId());
                })
                .flatMap(g -> ServerResponse.ok().bodyValue(g));
    }

    /**
     * DELETE /api/groups/{gid}/members/{uid}
     */
    Mono<ServerResponse> removeMember(ServerRequest req) {
        return groupService.removeMember(req.pathVariable("gid"), req.pathVariable("uid"))
                .flatMap(g -> ServerResponse.ok().bodyValue(g));
    }

    /**
     * GET /api/groups?userId=alice
     */
    Mono<ServerResponse> listGroupsForUser(ServerRequest req) {
        String userId = req.queryParam("userId").orElseThrow();
        return ServerResponse.ok().body(groupService.groupsForUser(userId), ChatGroup.class);
    }

    /**
     * GET /api/history/dm/{a}/{b}?limit=50
     */
    Mono<ServerResponse> dmHistory(ServerRequest req) {
        int limit = Integer.parseInt(req.queryParam("limit").orElse("50"));
        return ServerResponse.ok().body(
                messagingService.dmHistory(req.pathVariable("a"), req.pathVariable("b"), limit),
                ChatMessage.class
        );
    }

    /**
     * GET /api/history/group/{gid}?limit=50
     */
    Mono<ServerResponse> groupHistory(ServerRequest req) {
        int limit = Integer.parseInt(req.queryParam("limit").orElse("50"));
        return ServerResponse.ok().body(
                messagingService.groupHistory(req.pathVariable("gid"), limit),
                ChatMessage.class
        );
    }

    // ── Request body records ──────────────────────────────────────────────────
    @JsonIgnoreProperties(ignoreUnknown = true)
    record UserProvisionRequest(String userId) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CreateGroupRequest(String name, String createdBy, List<String> memberIds) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record MemberRequest(String userId) {
    }
}
package net.samitkumar.websocket_webflux;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
@Slf4j
public class WebsocketWebfluxApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebsocketWebfluxApplication.class, args);
	}

	@Bean
	Sinks.Many<String> sink() {
		return Sinks
				.many()
				.multicast()
				.onBackpressureBuffer();
	}

	@Bean
	public HandlerMapping handlerMapping(Sinks.Many<String> sink) {
		Map<String, WebSocketHandler> map = new HashMap<>();

		map.put("/ws", session -> {
			log.info("New WebSocket connection: sessionId={}", session.getId());

			return session
					.receive()
					.mapNotNull(WebSocketMessage::getPayloadAsText)
					.map(messageText -> "["+session.getAttributes().get("a_id")+"] : "+messageText)
					.doOnNext(sink::tryEmitNext)
					.then()
					.and(session
							.send(
									sink.asFlux().map(message -> {
										log.info("Sending message to sessionId={}: {}", session.getId(), message);
										return session.textMessage(message);
									})
							)
					);
		});

		return new SimpleUrlHandlerMapping(map, -1);
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
								log.info("HANDSHAKE - JOINED: sessionId={}", sessionId);
							})
							.doFinally(signal -> {
								log.info("HANDSHAKE - LEFT: sessionId={}, reason={}", sessionId, signal);
							});
				};

				return super.handleRequest(exchange, decoratedHandler);
			}
		};

		return new WebSocketHandlerAdapter(handshakeWebSocketService);
	}
}

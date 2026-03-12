package net.samitkumar.websocket_webflux;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import reactor.core.publisher.Sinks;

import java.util.HashMap;
import java.util.Map;

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
}

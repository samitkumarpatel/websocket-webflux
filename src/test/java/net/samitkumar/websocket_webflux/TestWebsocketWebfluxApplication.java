package net.samitkumar.websocket_webflux;

import org.springframework.boot.SpringApplication;

public class TestWebsocketWebfluxApplication {

	public static void main(String[] args) {
		SpringApplication.from(WebsocketWebfluxApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}

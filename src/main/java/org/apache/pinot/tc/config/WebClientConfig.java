package org.apache.pinot.tc.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Configuration
public class WebClientConfig {

    private static final Logger log = LoggerFactory.getLogger(WebClientConfig.class);

    @Bean("controller_client")
    public WebClient controllerClient(Environment environment) {
        return WebClient
                .builder()
                .baseUrl(environment.getProperty("pinot.controller.url", "http://localhost:9000"))
                .filters(exchangeFilterFunctions -> {
                    //exchangeFilterFunctions.add(logResponse());
                    exchangeFilterFunctions.add(errorHandler());
                })
                .build();
    }

    @Bean("broker_client")
    public WebClient brokerClient(Environment environment) {
        return WebClient
                .builder()
                .baseUrl(environment.getProperty("pinot.broker.url", "http://localhost:8099"))
                .filters(exchangeFilterFunctions -> {
                    //exchangeFilterFunctions.add(logResponse());
                    exchangeFilterFunctions.add(errorHandler());
                })
                .build();

    }

    private ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(WebClientConfig::logBody);
    }

    private static Mono<ClientResponse> logBody(ClientResponse response) {
        response.statusCode();

        return response.bodyToMono(String.class)
                .flatMap(body -> {
                    log.debug("Body is {}", body);
                    return Mono.just(response);
                });

    }

    private ExchangeFilterFunction errorHandler() {
        return ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
            if (clientResponse.statusCode().is5xxServerError() || clientResponse.statusCode().is4xxClientError()) {
                return clientResponse.bodyToMono(String.class)
                        .flatMap(errorBody -> Mono.error(new RuntimeException(errorBody)));
            } else {
                return Mono.just(clientResponse);
            }
        });
    }
}

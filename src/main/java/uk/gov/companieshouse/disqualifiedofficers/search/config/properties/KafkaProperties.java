package uk.gov.companieshouse.disqualifiedofficers.search.config.properties;

import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "spring.kafka")
@Component
public class KafkaProperties {

    private String bootstrapServers;
    private Map<String, String> authentication = new HashMap<>();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Map<String, String> getAuthentication() {
        return authentication;
    }

    public void setAuthentication(Map<String, String> authentication) {
        this.authentication = authentication;
    }

    @Override
    public String toString() {
        return "KafkaProperties{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", authentication=" + authentication +
                '}';
    }
}

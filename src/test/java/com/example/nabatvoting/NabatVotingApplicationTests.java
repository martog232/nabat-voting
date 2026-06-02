package com.example.nabatvoting;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"vote.cast"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@DirtiesContext
class NabatVotingApplicationTests {

    @Test
    void contextLoads() {
    }

}

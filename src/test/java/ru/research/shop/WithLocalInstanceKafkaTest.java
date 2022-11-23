package ru.research.shop;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.TestPropertySource;
import ru.research.shop.config.KafkaConsumerConfig;
import ru.research.shop.config.KafkaProducerConfig;
import ru.research.shop.service.UserKafkaConsumer;
import ru.research.shop.service.UserKafkaProducer;

import java.util.Date;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = {
        KafkaProducerConfig.class,
        UserKafkaProducer.class,
        KafkaConsumerConfig.class,
        UserKafkaConsumer.class
})
@TestPropertySource("classpath:application-test.properties")
public class WithLocalInstanceKafkaTest {

    @Autowired
    private UserKafkaProducer producer;

    @SpyBean
    private UserKafkaConsumer consumer;

    @Captor
    ArgumentCaptor<String> userArgumentCaptor;

    @Captor
    ArgumentCaptor<String> topicArgumentCaptor;

    @Captor
    ArgumentCaptor<Integer> partitionArgumentCaptor;

    @Captor
    ArgumentCaptor<Long> offsetArgumentCaptor;

    @Test
    void t() {
        System.out.println("---start---");

        var t1 = new Date().getTime();
        System.out.println(t1);
        producer.writeToKafka("user" + t1);

        var t2 = new Date().getTime();
        System.out.println(t2);
        producer.writeToKafka("user" + t2);

        var t3 = new Date().getTime();
        System.out.println(t3);
        producer.writeToKafka("user" + t3);

        verify(consumer, timeout(5000).times(3))
                .receiveCustomerEvent(userArgumentCaptor.capture(), topicArgumentCaptor.capture(),
                        partitionArgumentCaptor.capture(), offsetArgumentCaptor.capture());

        System.out.println("---stop---");
    }

}


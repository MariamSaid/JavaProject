package com.example.demo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DemoApplicationTests {
        @Autowired
        private WuzzufController controller;
	@Test
	void contextLoads() {
            Assertions.assertNotNull(controller);
	}

}

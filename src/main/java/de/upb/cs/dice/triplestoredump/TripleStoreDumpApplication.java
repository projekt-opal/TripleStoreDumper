package de.upb.cs.dice.triplestoredump;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TripleStoreDumpApplication implements CommandLineRunner {

    @Autowired
    private PaginationDumper dumper;

    public static void main(String[] args) {
        SpringApplication.run(TripleStoreDumpApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        dumper.dump();
    }
}

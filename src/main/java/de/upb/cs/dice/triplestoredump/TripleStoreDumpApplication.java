package de.upb.cs.dice.triplestoredump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class TripleStoreDumpApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(TripleStoreDumpApplication.class);

    @Autowired
    private PaginationDumper dumper;

    public static void main(String[] args) {
        SpringApplication.run(TripleStoreDumpApplication.class, args);
    }

    @Override
    public void run(String... args) {
        logger.info("Application started, arguments are {}", Arrays.toString(args));
        if (args.length > 0 && args[0].equals("dump"))
            try {
                dumper.dump();
            } catch (Exception e) {
                logger.error("{}", e);
            }
        logger.info("Application's job is finished");
    }
}

package de.upb.cs.dice.triplestoredump;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class IndexController {

    private static final Logger logger = LoggerFactory.getLogger(IndexController.class);

    @Autowired
    private PaginationDumper dumper;

    @GetMapping("/dump")
    public String getDump() {
        try {
            dumper.dump();
        } catch (Exception e) {
            logger.error("{}", e);
        }
        return "index";
    }
}

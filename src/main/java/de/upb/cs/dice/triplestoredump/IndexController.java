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
    private CrawlerTripleStoreDumper crawlerTripleStoreDumper;

    @Autowired
    private TripleStoreDumper tripleStoreDumper;

    @GetMapping("/dumpCrawler")
    public String getDump() {
        try {
            crawlerTripleStoreDumper.dump();
        } catch (Exception e) {
            logger.error("{}", e);
        }
        return "index";
    }

    @GetMapping("/dumpTripleStore")
    public String getDump2() {
        try {
            tripleStoreDumper.dump();
        } catch (Exception e) {
            logger.error("{}", e);
        }
        return "index";
    }
}

package de.upb.cs.dice.triplestoredump;

import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.retry.core.QueryExecutionFactoryRetry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@EnableScheduling
public class CrawlerTripleStoreDumper extends Dumper implements CredentialsProvider {

    private static final Logger logger = LoggerFactory.getLogger(CrawlerTripleStoreDumper.class);

    @Value("${tripleStore.url}")
    private String tripleStoreURL;
    @Value("${tripleStore.username}")
    private String tripleStoreUsername;
    @Value("${tripleStore.password}")
    private String tripleStorePassword;


    private org.apache.http.auth.Credentials credentials;

    void initialQueryExecutionFactory() {
        credentials = new UsernamePasswordCredentials(tripleStoreUsername, tripleStorePassword);

        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        clientBuilder.setDefaultCredentialsProvider(this);
        org.apache.http.impl.client.CloseableHttpClient client = clientBuilder.build();


        qef = new QueryExecutionFactoryHttp(
                tripleStoreURL, new org.apache.jena.sparql.core.DatasetDescription(), client);
        qef = new QueryExecutionFactoryRetry(qef, 5, 1000);

    }


    @Scheduled(cron = "${info.dumper.scheduler}")
    public void scheduledDumping() {
        try {
            dump();
        } catch (Exception e) {
            logger.error("{}", e);
        }
    }

    @Override
    boolean isTitleRepetitive(Resource dataSet, Model dataSetGraph) {
        Literal title = getTitle(dataSet, dataSetGraph);
        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT (COUNT(DISTINCT ?dataSet) AS ?num)\n" +
                "WHERE\n" +
                "{\n" +
                "  GRAPH ?g {\n" +
                "    ?dataSet  dct:title  ?title .\n" +
                "  }\n" +
                "}");
        pss.setNsPrefixes(PREFIXES);
        pss.setParam("title", title);
        long cnt = getCount(pss);
        return cnt > 1;
    }

    //should not throw exception
    @Override
    protected String getPortal(Resource dataSet) {
        String uri = dataSet.getURI();
        String[] split = uri.split("/");
        return split[2].substring(0, split[2].indexOf('.'));
    }


    @Override
    protected Model getAllPredicatesObjectsPublisherDistributions(Resource dataSet) {
        Model model;
        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "CONSTRUCT { " + "?dataSet ?predicate ?object .\n" +
                "\t?object ?p2 ?o2}\n" +
                "WHERE { \n" +
                "  GRAPH ?g {\n" +
                "    ?dataSet ?predicate ?object.\n" +
                "    OPTIONAL { ?object ?p2 ?o2 }\n" +
                "  }\n" +
                "}");

        pss.setNsPrefixes(PREFIXES);
        pss.setParam("dataSet", dataSet);

        model = executeConstruct(pss);
        return model;
    }

    @Override
    protected List<Resource> getListOfDataSets(int idx) {

        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT DISTINCT ?dataSet\n" +
                "WHERE { \n" +
                "  GRAPH ?g {\n" +
                "    ?dataSet a dcat:Dataset.\n" +
                "    FILTER(EXISTS{?dataSet dct:title ?title.})\n" +
                "  }\n" +
                "}\n" +
                "ORDER BY ?dataSet\n" +
                "OFFSET \n" + idx +
                "LIMIT " + PAGE_SIZE
        );

        pss.setNsPrefixes(PREFIXES);

        return getResources(pss);
    }


    /**
     * @return -1 => something went wrong, o.w. the number of distinct dataSets are return
     */
    @Override
    protected long getTotalNumberOfDataSets() {
        long cnt;
        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT (COUNT(DISTINCT ?dataSet) AS ?num)\n" +
                "WHERE { \n" +
                "  GRAPH ?g {\n" +
                "    ?dataSet a dcat:Dataset.\n" +
                "    FILTER(EXISTS{?dataSet dct:title ?title.})\n" +
                "  }\n" +
                "}");

        pss.setNsPrefixes(PREFIXES);

        cnt = getCount(pss);
        return cnt;
    }

    @Override
    public void setCredentials(AuthScope authScope, Credentials credentials) {

    }

    @Override
    public Credentials getCredentials(AuthScope authScope) {
        return credentials;
    }

    @Override
    public void clear() {

    }
}

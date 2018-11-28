package de.upb.cs.dice.triplestoredump;

import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.retry.core.QueryExecutionFactoryRetry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.jena.query.ParameterizedSparqlString;
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
    protected List<Resource> getAllDataSetsForTitle(String title) {

        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT DISTINCT ?dataSet \n" +
                "WHERE\n" +
                "  { GRAPH ?g\n" +
                "      { \n" +
                "        ?dataSet dct:title ?title" +
                "      }\n" +
                "  }\n" +
                "ORDER BY ?dataSet");

        pss.setNsPrefixes(PREFIXES);
        pss.setParam("title", ResourceFactory.createStringLiteral(title));
        return getResources(pss);
    }

    @Override
    protected List<String> getNotUniqueTitles() {

        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT ?title \n" +
                "WHERE\n" +
                "  { GRAPH ?g\n" +
                "      { \n" +
                "        ?dataSet a dcat:Dataset .\n" +
                "        ?dataSet dct:title ?title .\n" +
                "      }\n" +
                "  }\n" +
                "GROUP BY ?title \n" +
                "HAVING (COUNT(DISTINCT ?dataSet ) > 1)");

        pss.setNsPrefixes(PREFIXES);

        return getTitles(pss);
    }

    //should not throw exception
    @Override
    protected String getPortal(Resource dataSet) {
        String uri = dataSet.getURI();
        String[] split = uri.split("/");
        return split[2].substring(0, split[2].indexOf('.'));
    }

    @Override
    protected Literal getTitle(Resource dataSet, Model dataSetGraph) {
        return dataSetGraph.getRequiredProperty(dataSet, DCTerms.title).getLiteral();
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

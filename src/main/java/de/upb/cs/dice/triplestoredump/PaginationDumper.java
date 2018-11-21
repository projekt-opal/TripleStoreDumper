package de.upb.cs.dice.triplestoredump;

import com.google.common.collect.ImmutableMap;
import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.retry.core.QueryExecutionFactoryRetry;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
@EnableScheduling
public class PaginationDumper implements CredentialsProvider {

    private static final Logger logger = LoggerFactory.getLogger(PaginationDumper.class);


    @Value("${tripleStore.url}")
    private String tripleStoreURL;
    @Value("${tripleStore.username}")
    private String tripleStoreUsername;
    @Value("${tripleStore.password}")
    private String tripleStorePassword;

    @Value("${internalFileServer.address}")
    private String serverAddress;

    @Value("${output.folderPath}")
    private String folderPath;

    private org.apache.http.auth.Credentials credentials;

    private org.aksw.jena_sparql_api.core.QueryExecutionFactory qef;


    private static final ImmutableMap<String, String> PREFIXES = ImmutableMap.<String, String>builder()
            .put("dcat", "http://www.w3.org/ns/dcat#")
            .put("dct", "http://purl.org/dc/terms/")
            .build();
    private static final int PAGE_SIZE = 100;



    @Autowired
    private InfoDataSetRepository infoDataSetRepository;



    @Scheduled(cron = "${info.dumper.scheduler}")
    public void scheduledDumping() {
        try {
            dump();
        } catch (Exception e) {
            logger.error("{}", e);
        }
    }

    @PostConstruct
    public void initializeAuthenticationAndQueryExecution() {
        initialQueryExecutionFactory();
    }

    private void initialQueryExecutionFactory() {
        credentials = new UsernamePasswordCredentials(tripleStoreUsername, tripleStorePassword);

        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        clientBuilder.setDefaultCredentialsProvider(this);
        org.apache.http.impl.client.CloseableHttpClient client = clientBuilder.build();


        qef = new QueryExecutionFactoryHttp(
                tripleStoreURL, new org.apache.jena.sparql.core.DatasetDescription(), client);
        qef = new QueryExecutionFactoryRetry(qef, 5, 1000);
//        qef = new QueryExecutionFactoryDelay(qef, 100);

    }

    public void dump() throws Exception {


        logger.info("dumping is started");

        //First, get the total number of datasets in the triple store.
        //Then, get names of datasets from triple store page by page (100 dataset in each request)
        //After that, for each of those 100 datasets get all predicate and object and also dcat:publisher and dcat:Distribution related to that dataset,
        //Finally, generate a file for all those 100 datasets and mention the address for the next page

//        initialQueryExecutionFactory();


        long totalNumberOfDataSets = getTotalNumberOfDataSets();
        logger.debug("Total number of datasets is {}", totalNumberOfDataSets);
        if (totalNumberOfDataSets == -1) {
            throw new Exception("Cannot Query the TripleStore");
        }

        Resource opal = ResourceFactory.createResource("http://projekt-opal.de/opal");

        for (int idx = 0; idx < totalNumberOfDataSets; idx += PAGE_SIZE) {
            List<Resource> listOfDataSets = getListOfDataSets(idx);

            if (listOfDataSets.size() != PAGE_SIZE && listOfDataSets.size() != totalNumberOfDataSets - idx) {
                throw new Exception("There is an error in getting dataSets from TripleStore");
            }

            Model model = ModelFactory.createDefaultModel();
            model.add(opal, RDF.type, DCAT.Catalog);

            //get graph for each dataSet
            for (Resource dataSet : listOfDataSets) {
                Model dataSetGraph = getAllPredicatesObjectsPublisherDistributions(dataSet);
                if (dataSetGraph == null) {
                    throw new Exception("There is an error in getting " + dataSet + " graph");
                }
                //CKAN specific ( title in the CKAN is the key)
                if(titleIsRepetitive(dataSet, dataSetGraph)) {
                    String title = getTitle(dataSet, dataSetGraph).getString();
                    String portal = getPortal(dataSet, dataSetGraph);
                    Optional<InfoDataSet> info = infoDataSetRepository.findByTitleAndPortal(title, portal);
                    dataSetGraph.remove(dataSetGraph.getRequiredProperty(dataSet, DCTerms.title));
                    InfoDataSet infoDataSet;
                    if(info.isPresent()) {
                        infoDataSet = info.get();
                        infoDataSet.setCnt(infoDataSet.getCnt() + 1);
                    } else infoDataSet = new InfoDataSet(title, portal, 1);
                    infoDataSetRepository.save(infoDataSet);
                    dataSetGraph.add(dataSet, DCTerms.title, ResourceFactory.createStringLiteral(
                            String.format("%s (%s_%d)", title, portal, infoDataSet.getCnt())));
                }
                model.add(dataSetGraph);
                model.add(opal, DCAT.dataset, dataSet);
            }

            //add pagination info
            String addressPattern = serverAddress + "/model%d.ttl";
            Resource thisPageAddress = ResourceFactory.createResource(String.format(addressPattern, (idx / PAGE_SIZE + 1)));
            model.add(thisPageAddress, RDF.type, NS4.PagedCollection);
            model.add(thisPageAddress, NS4.firstPage,
                    ResourceFactory.createResource(String.format(addressPattern, 1)));
            if (idx + PAGE_SIZE < totalNumberOfDataSets)
                model.add(thisPageAddress, NS4.nextPage,
                        ResourceFactory.createResource(String.format(addressPattern, ((idx / PAGE_SIZE + 1) + 1))));
            model.add(thisPageAddress, NS4.lastPage,
                    ResourceFactory.createResource(String.format(addressPattern, (totalNumberOfDataSets / PAGE_SIZE + 1))));
            model.add(thisPageAddress, NS4.itemsPerPage, ResourceFactory.createTypedLiteral(PAGE_SIZE));
            model.add(thisPageAddress, NS4.totalItems, ResourceFactory.createTypedLiteral(totalNumberOfDataSets));

            //write model
            String fileName = String.format("model%d.ttl", (idx / PAGE_SIZE + 1));
            try (FileWriter out = new FileWriter(folderPath + "/" + fileName)) {
                model.write(out, "TURTLE");
            }
        }
    }

    //should not throw exception
    private String getPortal(Resource dataSet, Model dataSetGraph) {
        String uri = dataSet.getURI();
        String[] split = uri.split("/");
        return split[2].substring(0, split[2].indexOf('.'));
    }

    private boolean titleIsRepetitive(Resource dataSet, Model dataSetGraph) {

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
        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            ResultSet resultSet = queryExecution.execSelect();
            if(resultSet.hasNext()) {
                int num = resultSet.nextSolution().get("num").asLiteral().getInt();
                return num > 1;
            }
        }
        return false;
    }

    private Literal getTitle(Resource dataSet, Model dataSetGraph) {
        return dataSetGraph.getRequiredProperty(dataSet, DCTerms.title).getLiteral();
    }

    private Model getAllPredicatesObjectsPublisherDistributions(Resource dataSet) {
        Model model = null;
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

        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            model = queryExecution.execConstruct();
        }
        return model;
    }

    private List<Resource> getListOfDataSets(int idx) {

        List<Resource> ret = new ArrayList<>();

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

        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            ResultSet resultSet = queryExecution.execSelect();
            while (resultSet.hasNext()) {
                QuerySolution solution = resultSet.nextSolution();
                Resource dataSet = solution.getResource("dataSet");
                ret.add(dataSet);
            }
        }
        return ret;
    }


    /**
     * @return -1 => something went wrong, o.w. the number of distinct dataSets are return
     */
    private long getTotalNumberOfDataSets() {
        long cnt = -1;
        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT (COUNT(DISTINCT ?dataSet) AS ?num)\n" +
                "WHERE { \n" +
                "  GRAPH ?g {\n" +
                "    ?dataSet a dcat:Dataset.\n" +
                "    FILTER(EXISTS{?dataSet dct:title ?title.})\n" +
                "  }\n" +
                "}");

        pss.setNsPrefixes(PREFIXES);

        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            ResultSet resultSet = queryExecution.execSelect();
            while (resultSet.hasNext()) {
                QuerySolution solution = resultSet.nextSolution();
                RDFNode num = solution.get("num");
                cnt = num.asLiteral().getLong();
            }
        }
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

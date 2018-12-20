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
public class Dumper implements CredentialsProvider {
    private static final Logger logger = LoggerFactory.getLogger(Dumper.class);


    private static final ImmutableMap<String, String> PREFIXES = ImmutableMap.<String, String>builder()
            .put("dcat", "http://www.w3.org/ns/dcat#")
            .put("dct", "http://purl.org/dc/terms/")
            .build();

    private static final int PAGE_SIZE = 5000;

    @Value("${output.folderPath}")
    private String folderPath;

    @Value("${internalFileServer.address}")
    private String serverAddress;

    private org.aksw.jena_sparql_api.core.QueryExecutionFactory qef;
    @Value("${tripleStore.url}")
    private String tripleStoreURL;
    @Value("${tripleStore.username}")
    private String tripleStoreUsername;
    @Value("${tripleStore.password}")
    private String tripleStorePassword;
    private org.apache.http.auth.Credentials credentials;

    private final InfoDataSetRepository infoDataSetRepository;

    @Autowired
    public Dumper(InfoDataSetRepository infoDataSetRepository) {
        this.infoDataSetRepository = infoDataSetRepository;
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

    }

//    @Scheduled(cron = "${info.dumper.scheduler}")
//    public void scheduledDumping() {
//        try {
//            dump();
//        } catch (Exception e) {
//            logger.error("{}", e);
//        }
//    }

    void dump() throws Exception {


        logger.info("dumping is started");

        infoDataSetRepository.deleteAll();

        //First, get the total number of datasets in the triple store.
        //Then, get names of datasets from triple store page by page (100 dataset in each request)
        //After that, for each of those 100 datasets get all predicate and object and also dcat:publisher and dcat:Distribution related to that dataset,
        //Finally, generate a file for all those 100 datasets and mention the address for the next page


        long totalNumberOfDataSets = getTotalNumberOfDataSets();
        logger.debug("Total number of datasets is {}", totalNumberOfDataSets);
        if (totalNumberOfDataSets == -1) {
            throw new Exception("Cannot Query the TripleStore");
        }

        for (int idx = 0; idx < totalNumberOfDataSets; idx += PAGE_SIZE) {
            List<Resource> listOfDataSets = getListOfDataSets(idx, (int) Math.min(totalNumberOfDataSets - idx, PAGE_SIZE));

            if (listOfDataSets.size() != PAGE_SIZE && listOfDataSets.size() != totalNumberOfDataSets - idx) {
                throw new Exception("There is an error in getting dataSets from TripleStore");
            }

            Model model = ModelFactory.createDefaultModel();

            //get graph for each dataSet
            for (Resource dataSet : listOfDataSets) {

                Resource portal = getPortal(dataSet);
                if(portal == null) {
                    logger.warn("portal is null for dataset {}", dataSet);
                    continue;
                }

                //probably the same portal will be added multiple times (but it will be ignored automatically)
                model.add(portal, RDF.type, DCAT.Catalog);


                Model dataSetGraph = getAllPredicatesObjectsPublisherDistributions(dataSet);
                if (dataSetGraph == null) {
                    throw new Exception("There is an error in getting " + dataSet + " graph");
                }

                //CKAN specific ( title in the CKAN is the key)
                if (isTitleRepetitive(dataSet, dataSetGraph)) {
                    String title = getTitle(dataSet, dataSetGraph).getString();
                    String portalName = getPortalName(portal);
                    Optional<InfoDataSet> info = infoDataSetRepository.findByTitleAndPortal(title, portalName);
                    dataSetGraph.remove(dataSetGraph.getRequiredProperty(dataSet, DCTerms.title));
                    InfoDataSet infoDataSet;
                    if (info.isPresent()) {
                        infoDataSet = info.get();
                        infoDataSet.setCnt(infoDataSet.getCnt() + 1);
                    } else infoDataSet = new InfoDataSet(title, portalName, 1);
                    infoDataSetRepository.save(infoDataSet);
                    String generatedTitle = String.format("%s (%s_%d)", title, portal, infoDataSet.getCnt());
                    dataSetGraph.add(dataSet, DCTerms.title, ResourceFactory.createStringLiteral(generatedTitle));
                    logger.trace("generated title is {}", generatedTitle);
                }
                model.add(dataSetGraph);
                model.add(portal, DCAT.dataset, dataSet);
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

    private String getPortalName(Resource portal) {
        String[] split = portal.getURI().split("/");// TODO: 19.12.18 with substring would e faster
        return split[split.length - 1];
    }

    private Literal getTitle(Resource dataSet, Model dataSetGraph) {
        return dataSetGraph.getRequiredProperty(dataSet, DCTerms.title).getLiteral();
    }

    private Resource getPortal(Resource dataSet) {
        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT ?portal\n" +
                "WHERE\n" +
                "{\n" +
                "  GRAPH ?g {\n" +
                "    ?portal dcat:dataset ?dataSet .\n" +
                "  }\n" +
                "}");
        pss.setNsPrefixes(PREFIXES);
        pss.setParam("dataSet", dataSet);

        List<Resource> resources = getResources(pss, "portal");

        if (resources.size() != 1) {
            logger.error("non unique catalog for dataset {}, {}", dataSet, resources);
            return null;
        }
        return resources.get(0);
    }

    private boolean isTitleRepetitive(Resource dataSet, Model dataSetGraph) {
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

    private Model getAllPredicatesObjectsPublisherDistributions(Resource dataSet) {
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


    private Model executeConstruct(ParameterizedSparqlString pss) {
        Model model = null;
        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            model = queryExecution.execConstruct();
        } catch (Exception ex) {
            logger.error("An error occurred in executing construct, {}", ex);
        }
        return model;
    }

    private List<Resource> getListOfDataSets(int idx, int limit) {

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
                "LIMIT " + limit
        );

        pss.setNsPrefixes(PREFIXES);

        return getResources(pss, "dataSet");
    }

    private List<Resource> getResources(ParameterizedSparqlString pss, String resourceVariable) {
        List<Resource> ret = new ArrayList<>();
        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            ResultSet resultSet = queryExecution.execSelect();
            while (resultSet.hasNext()) {
                QuerySolution solution = resultSet.nextSolution();
                Resource resource = solution.getResource(resourceVariable);
                ret.add(resource);
                logger.trace("getResource: {}", resource);
            }
        } catch (Exception ex) {
            logger.error("An error occurred in getting resources, {}", ex);
        }
        return ret;
    }

    /**
     * @return -1 => something went wrong, o.w. the number of distinct dataSets are return
     */
    private long getTotalNumberOfDataSets() {
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

    private long getCount(ParameterizedSparqlString pss) {
        long cnt = -1;
        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            ResultSet resultSet = queryExecution.execSelect();
            while (resultSet.hasNext()) {
                QuerySolution solution = resultSet.nextSolution();
                RDFNode num = solution.get("num");
                cnt = num.asLiteral().getLong();
            }
        } catch (Exception ex) {
            logger.error("An error occurred in getting Count, {}", ex);
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

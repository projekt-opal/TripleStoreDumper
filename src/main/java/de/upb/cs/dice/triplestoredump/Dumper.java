package de.upb.cs.dice.triplestoredump;

import com.google.common.collect.ImmutableMap;
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
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public abstract class Dumper {
    private static final Logger logger = LoggerFactory.getLogger(Dumper.class);


    protected static final ImmutableMap<String, String> PREFIXES = ImmutableMap.<String, String>builder()
            .put("dcat", "http://www.w3.org/ns/dcat#")
            .put("dct", "http://purl.org/dc/terms/")
            .build();

    protected static final int PAGE_SIZE = 100;

    @Value("${output.folderPath}")
    private String folderPath;

    @Value("${internalFileServer.address}")
    private String serverAddress;


    @Autowired
    private InfoDataSetRepository infoDataSetRepository;

    @PostConstruct
    public void initializeAuthenticationAndQueryExecution() {
        initialQueryExecutionFactory();
    }

    org.aksw.jena_sparql_api.core.QueryExecutionFactory qef;

    abstract void initialQueryExecutionFactory();

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

        List<String> notUniqueTitles = getNotUniqueTitles();
        adjustTitles(notUniqueTitles);

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
                Optional<InfoDataSet> info = titleIsRepetitive(dataSet, dataSetGraph);
                if (info.isPresent()) {
                    dataSetGraph.remove(dataSetGraph.getRequiredProperty(dataSet, DCTerms.title));
                    dataSetGraph.add(dataSet, DCTerms.title, info.get().getGeneratedTitle());
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

    private void adjustTitles(List<String> notUniqueTitles) {
        for (String title : notUniqueTitles) {
            logger.trace("generating title for : {}", title);
            List<Resource> allDataSetsForTitle = getAllDataSetsForTitle(title);
            for (Resource dataSet : allDataSetsForTitle) {
                String portal = getPortal(dataSet);
                List<InfoDataSet> info = infoDataSetRepository.findByTitleAndPortal(title, portal); //optimize it by just returning the size
                InfoDataSet infoDataSet = new InfoDataSet(title, portal, dataSet.getURI());
                infoDataSet.setGeneratedTitle
                        (String.format("%s (%s_%d)", title, infoDataSet.getPortal(), info.size() + 1));
                infoDataSetRepository.save(infoDataSet);
                logger.trace("generated title for {} is {}", title, infoDataSet.getGeneratedTitle());
            }
        }
    }

    protected abstract List<Resource> getAllDataSetsForTitle(String title);

    protected abstract List<String> getNotUniqueTitles();

    List<String> getTitles(ParameterizedSparqlString pss) {
        List<String> ret = new ArrayList<>();
        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            ResultSet resultSet = queryExecution.execSelect();
            while (resultSet.hasNext()) {
                QuerySolution solution = resultSet.nextSolution();
                String title = solution.get("title").asLiteral().getString();
                ret.add(title);
                logger.trace("getTitle: {}", title);
            }
        } catch (Exception ex) {
            logger.error("An error occurred in getting titles, {}", ex);
        }

        return ret;
    }

    //should not throw exception
    protected abstract String getPortal(Resource dataSet);

    private Optional<InfoDataSet> titleIsRepetitive(Resource dataSet, Model dataSetGraph) {

        String title = getTitle(dataSet, dataSetGraph).getString();
        String portal = getPortal(dataSet);
        Optional<InfoDataSet> info = infoDataSetRepository.findByUri(dataSet.getURI());
        return info;
    }

    protected abstract Literal getTitle(Resource dataSet, Model dataSetGraph);

    protected abstract Model getAllPredicatesObjectsPublisherDistributions(Resource dataSet);

    Model executeConstruct(ParameterizedSparqlString pss) {
        Model model = null;
        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            model = queryExecution.execConstruct();
        }  catch (Exception ex) {
            logger.error("An error occurred in executing construct, {}", ex);
        }
        return model;
    }

    protected abstract List<Resource> getListOfDataSets(int idx);

    List<Resource> getResources(ParameterizedSparqlString pss) {
        List<Resource> ret = new ArrayList<>();
        try (QueryExecution queryExecution = qef.createQueryExecution(pss.asQuery())) {
            ResultSet resultSet = queryExecution.execSelect();
            while (resultSet.hasNext()) {
                QuerySolution solution = resultSet.nextSolution();
                Resource dataSet = solution.getResource("dataSet");
                ret.add(dataSet);
                logger.trace("getResource: {}", dataSet);
            }
        }  catch (Exception ex) {
            logger.error("An error occurred in getting resources, {}", ex);
        }
        return ret;
    }

    protected abstract long getTotalNumberOfDataSets();

    long getCount(ParameterizedSparqlString pss) {
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


}
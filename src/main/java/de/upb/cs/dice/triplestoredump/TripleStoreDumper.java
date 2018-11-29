package de.upb.cs.dice.triplestoredump;

import org.aksw.jena_sparql_api.http.QueryExecutionFactoryHttp;
import org.aksw.jena_sparql_api.retry.core.QueryExecutionFactoryRetry;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TripleStoreDumper extends Dumper {
    @Override
    void initialQueryExecutionFactory() {

        qef = new QueryExecutionFactoryHttp("https://www.europeandataportal.eu/sparql"); // TODO: 28.11.18 "Get from DB or file"
        qef = new QueryExecutionFactoryRetry(qef, 5, 1000);
    }

    @Override
    protected String getPortal(Resource dataSet) {
        return "europeanDataPortal.de"; // TODO: 28.11.18 Get the portal details to be returned
    }


    @Override
    protected Model getAllPredicatesObjectsPublisherDistributions(Resource dataSet) {
        Model model;
        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "CONSTRUCT { " + "?dataSet ?predicate ?object .\n" +
                "\t?object ?p2 ?o2}\n" +
                "WHERE { \n" +
                "    ?dataSet ?predicate ?object.\n" +
                "    OPTIONAL { ?object ?p2 ?o2 }\n" +
                "}");

        pss.setNsPrefixes(PREFIXES);
        pss.setParam("dataSet", dataSet);

        model = executeConstruct(pss);
        return model;
    }

    @Override
    boolean isTitleRepetitive(Resource dataSet, Model dataSetGraph) {
        Literal title = getTitle(dataSet, dataSetGraph);
        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT (COUNT(DISTINCT ?dataSet) AS ?num)\n" +
                "WHERE\n" +
                "{\n" +
                "    ?dataSet  dct:title  ?title .\n" +
                "}");
        pss.setNsPrefixes(PREFIXES);
        pss.setParam("title", title);
        long cnt = getCount(pss);
        return cnt > 1;
    }


    @Override
    protected List<Resource> getListOfDataSets(int idx) {

        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT DISTINCT ?dataSet\n" +
                "WHERE { \n" +
                "    ?dataSet a dcat:Dataset.\n" +
                "    FILTER(EXISTS{?dataSet dct:title ?title.})\n" +
                "}\n" +
                "ORDER BY ?dataSet\n" +
                "OFFSET \n" + idx +
                "LIMIT " + PAGE_SIZE
        );

        pss.setNsPrefixes(PREFIXES);

        return getResources(pss);
    }

    @Override
    protected long getTotalNumberOfDataSets() {

        long cnt;
        ParameterizedSparqlString pss = new ParameterizedSparqlString("" +
                "SELECT (COUNT(DISTINCT ?dataSet) AS ?num)\n" +
                "WHERE { \n" +
                "    ?dataSet a dcat:Dataset.\n" +
                "    FILTER(EXISTS{?dataSet dct:title ?title.})\n" +
                "}");

        pss.setNsPrefixes(PREFIXES);

        cnt = getCount(pss);
        return cnt;
    }
}
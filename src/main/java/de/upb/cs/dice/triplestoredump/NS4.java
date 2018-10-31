package de.upb.cs.dice.triplestoredump;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public class NS4 {
    private static final Model m_model = ModelFactory.createDefaultModel();

    /** <p>The namespace of the vocabulary as a string</p> */
    public static final String NS = "http://www.w3.org/ns/hydra/core#"; //todo appropriate NS should be chosen

    /** <p>The namespace of the vocabulary as a string</p>
     *  @see #NS */
    public static String getURI() {return NS;}

    /** <p>The namespace of the vocabulary as a resource</p> */
    public static final Resource NAMESPACE = m_model.createResource( NS );

    /** <p>A summary of the resource.</p> */
    public static final Resource PagedCollection = m_model.createResource( NS + "PagedCollection" );
    public static final Property firstPage = m_model.createProperty(NS + "firstPage" );
    public static final Property nextPage = m_model.createProperty(NS + "nextPage" );
    public static final Property lastPage = m_model.createProperty(NS + "lastPage" );
    public static final Property totalItems = m_model.createProperty(NS + "totalItems" );
    public static final Property itemsPerPage = m_model.createProperty(NS + "itemsPerPage" );

}

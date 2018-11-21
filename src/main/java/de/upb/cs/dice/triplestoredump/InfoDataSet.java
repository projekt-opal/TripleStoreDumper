package de.upb.cs.dice.triplestoredump;

import javax.persistence.*;

@Entity
public class InfoDataSet {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private long id;

    @Column(length = 2000)
    private String title;

    @Column
    private String portal;

    @Column
    private String uri;

    @Column(length = 2000)
    private String generatedTitle;

    public InfoDataSet() {
    }

    public InfoDataSet(String title, String portal, String uri) {
        this.title = title;
        this.portal = portal;
        this.uri = uri;
    }

    public long getId() {
        return id;
    }

    public InfoDataSet setId(long id) {
        this.id = id;
        return this;
    }

    public String getTitle() {
        return title;
    }

    public InfoDataSet setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getPortal() {
        return portal;
    }

    public InfoDataSet setPortal(String portal) {
        this.portal = portal;
        return this;
    }

    public String getUri() {
        return uri;
    }

    public InfoDataSet setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getGeneratedTitle() {
        return generatedTitle;
    }

    public InfoDataSet setGeneratedTitle(String generatedTitle) {
        this.generatedTitle = generatedTitle;
        return this;
    }
}

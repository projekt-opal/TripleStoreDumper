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
    private int cnt;

    public InfoDataSet() {
    }

    public InfoDataSet(String title, String portal, int cnt) {
        this.title = title;
        this.portal = portal;
        this.cnt = cnt;
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

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }
}

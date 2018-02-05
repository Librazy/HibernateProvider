package org.librazy.provider.hibernate;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.util.Date;
import java.util.UUID;

@Entity
@Table(name = "TIMEKEEPER")
public class TestEntity {

    private String test;

    private UUID uuid;

    private int id;

    private Date dateTime;

    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    @Column(name = "uuid", length = 36)
    public UUID getUuid() {
        return uuid;
    }

    public TestEntity setUuid(UUID uuid) {
        this.uuid = uuid;
        return this;
    }

    @Id
    @Column(name = "id", nullable = false)
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public int getId() {
        return id;
    }

    public TestEntity setId(int id) {
        this.id = id;
        return this;
    }

    @CreationTimestamp
    @Column(name = "timestamp")
    @Temporal(TemporalType.TIMESTAMP)
    public Date getDateTime() {
        return dateTime;
    }

    public TestEntity setDateTime(Date dateTime) {
        this.dateTime = dateTime;
        return this;
    }

    @Column(name = "test")
    public String getTest() {
        return test;
    }

    public TestEntity setTest(String test) {
        this.test = test;
        return this;
    }
}

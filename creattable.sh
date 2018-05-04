CREATE KEYSPACE IF NOT EXISTS Energy_consumption WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };


CREATE TABLE Energy_consumption.rawdata (
    Time varchar,
    ID varchar,
    Business varchar,
    Scale varchar,
    State varchar,
    Ecoll float,
    Efacility float,
    Efan float,
    Gfacility float,
    Eheat float,
    Gheat float,
    Einterequip float,
    Ginterequip float,
    Einterlight float,
    Gwater float,
    Etotal float,
    Gtotal float,
    PRIMARY KEY (State, (ID, Time)));

    CREATE TABLE Energy_consumption.statedata (
    Time varchar,
    State varchar,
    Ecoll float,
    Efacility float,
    Efan float,
    Gfacility float,
    Eheat float,
    Gheat float,
    Einterequip float,
    Ginterequip float,
    Einterlight float,
    Gwater float,
    Etotal float,
    Gtotal float,
    PRIMARY KEY (Time, State)
    );
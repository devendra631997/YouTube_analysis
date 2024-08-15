CREATE TABLE Channels (
    ID int NOT NULL,
    PRIMARY KEY (ID),
    Name varchar(255),
    Address varchar(255),
    totalSubscriber int,
);

CREATE TABLE Videos (
    ID int NOT NULL,
    PRIMARY KEY (ID),
    Name varchar(255),
    Views int,
    Likes int,
    Comments int,
    ChannelId int,
    FOREIGN KEY (ChannelId) REFERENCES Channels(ID)
);
CREATE TABLE SELECT_REQUESTS(
    ID INT AUTO_INCREMENT PRIMARY KEY,
    QUERY varchar(255) NOT NULL,
    RECEIVER varchar(25),
    EVENT_TYPE varchar(25) NOT NULL,
    NOTIFICATION_LEVEL varchar(50) NOT NULL,
    QUERY_DESCRIPTION varchar(255) NOT NULL,
    MESSAGE varchar(255) NOT NULL,
    CONDITION BIGINT
);

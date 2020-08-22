
DROP TABLE businesses;
DROP TABLE users;

CREATE TABLE users(
    userId INT NOT NULL AUTO_INCREMENT,
    username varchar(45) NOT NULL,
    password varchar(45) NOT NULL,
    firstname varchar(45) NOT NULL,
    lastname varchar(45) NOT NULL,
    PRIMARY KEY (userId)
);

CREATE TABLE businesses(
    businessId INT NOT NULL AUTO_INCREMENT,
    firstname varchar(45) NOT NULL,
    PRIMARY KEY (businessId)
);

CREATE TABLE user_to_business(
    linkId INT NOT NULL AUTO_INCREMENT,
    userId INT NOT NULL,
    businessId INT NOT NULL,
    active BIT NOT NULL,
    PRIMARY KEY (linkId),
    FOREIGN KEY (userId) REFERENCES users(userId),
    FOREIGN KEY (businessId) REFERENCES businesses(businessId)
);

CREATE TABLE adaccounts(
    adAccountId INT NOT NULL AUTO_INCREMENT,
    businessId INT NOT NULL,
    name varchar(45) NOT NULL,
    platform varchar(45) NOT NULL,
    PRIMARY KEY (adAccountId),
    FOREIGN KEY (businessId) REFERENCES businesses(businessId)
);

CREATE TABLE campaigns(
    campaignId INT NOT NULL AUTO_INCREMENT,
    adAccountId INT NOT NULL,
    name varchar(45) NOT NULL,
    platform varchar(45) NOT NULL,
    platformId varchar(45) NOT NULL,
    PRIMARY KEY (campaignId),
    FOREIGN KEY (adAccountId) REFERENCES adaccounts(adAccountId)
);

CREATE TABLE adsets(
    adSetId INT NOT NULL AUTO_INCREMENT,
    campaignId INT NOT NULL,
    name varchar (45) NOT NULL,
    platform varchar(45) NOT NULL,
    platformId varchar(45) NOT NULL,
    PRIMARY KEY (adSetId),
    FOREIGN KEY (campaignId) REFERENCES campaigns(campaignId)
);

CREATE TABLE ads (
    adId INT NOT NULL AUTO_INCREMENT,
    adSetId INT NOT NULL,
    platform varchar(45) NOT NULL,
    platformId varchar(45) NOT NULL,
    PRIMARY KEY (adId),
    FOREIGN KEY (adSetId) REFERENCES adsets(adSetId)
);


CREATE TABLE adevents(
    eventId INT NOT NULL AUTO_INCREMENT,
    clientId INT NOT NULL,
    adId INT NOT NULL,
    ipAddress varchar(45) NOT NULL,
    type varchar(45) NOT NULL,
    PRIMARY KEY (eventId),
    FOREIGN KEY (adId) REFERENCES ads(adId)
);

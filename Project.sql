# Create database
CREATE DATABASE cryptocurrency;

# Set default schema
USE cryptocurrency;

# Creation of crypto_main table for loading data from the cleaned dataset
    CREATE TABLE crypto_main (
    id int NOT NULL,
    cmc_name varchar(60),
    symbol varchar(15),
    num_market_pairs int,
    date_added datetime,
    max_supply double,
    circulating_supply double,
    total_supply double,
    cmc_rank int,
    price double,
    volume_24h double ,
    volume_change_24h double,
    percent_change_1h double,
    percent_change_24h double,
    percent_change_7d double,
    percent_change_30d double,
    percent_change_60d double,
    percent_change_90d double,
    market_cap double,
    market_cap_dominance double,
    fully_diluted_market_cap double,
    mineable_tag varchar(3),
    exchange_tag varchar(3),
    payments_tag varchar(5),
    PRIMARY KEY (id)
 ) ;
 
# Load data into crypto_main table
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/crypto_data.csv'
INTO TABLE crypto_main
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SELECT * FROM crypto_main;

# Create table cryptodata containing cryptocurrency information
CREATE TABLE cryptodata (
	Id int NOT NULL,
	cmc_name varchar(60) ,
	Symbol varchar(15) ,
	Date_added datetime ,
    cmc_rank int ,
    num_market_pairs int ,
    price double ,
    market_cap double ,
    PRIMARY KEY (Id)
 ) ;
	
# Insert data into cryptodata table
INSERT into cryptodata (id, cmc_name, Symbol, Date_added, cmc_rank, num_market_pairs, price, market_cap)
SELECT id, cmc_name, Symbol, Date_added, cmc_rank, num_market_pairs, price, market_cap FROM crypto_main;

# Retrieve data from cryptodata table
SELECT * FROM cryptodata;

# Create table cryptovolch containing crypto volume change information
CREATE TABLE cryptovolch (
    id int NOT NULL,
    volume_24h double ,
    volume_change_24h double,
    PRIMARY KEY (id),
    FOREIGN KEY (id) REFERENCES cryptodata(Id)
 ) ;

# Insert data into cryptovolch table
INSERT into cryptovolch (id, volume_24h, volume_change_24h)
SELECT id, volume_24h, volume_change_24h FROM crypto_main;

# Retrieve data from cryptovolch table
SELECT * FROM cryptovolch;

# Create table cryptopctch containing crypto percentage change information
CREATE TABLE cryptopctch (
	id int NOT NULL,
    percent_change_1h double  ,
    percent_change_24h double ,
    percent_change_7d double  ,
    percent_change_30d double ,
    percent_change_60d double ,
    percent_change_90d double ,
    PRIMARY KEY (id),
    FOREIGN KEY (id) REFERENCES cryptodata (Id)
 );
 
 # Insert data into cryptopctch table
INSERT into cryptopctch (id, percent_change_1h, percent_change_24h, percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d)
SELECT id, percent_change_1h, percent_change_24h, percent_change_7d, percent_change_30d, percent_change_60d, percent_change_90d FROM crypto_main;

# Retrieve data from cryptopctch table
SELECT * FROM cryptopctch;

# Create table cryptosupply containing crypto supply information
CREATE TABLE cryptosupply (
    id int NOT NULL,
    max_supply double DEFAULT NULL,
    circulating_supply double DEFAULT NULL,
    total_supply double DEFAULT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (id) REFERENCES cryptodata(Id)
 ) ;

# Insert data into cryptosupply table
INSERT into cryptosupply (id, max_supply, circulating_supply, total_supply)
SELECT id, max_supply, circulating_supply, total_supply FROM crypto_main;

# Retrieve data from cryptosupply table
SELECT * FROM cryptosupply;

# Create table cryptotag containing crypto tag information
CREATE TABLE cryptotag (
    id int NOT NULL,
    mineable_tag varchar(3),
    exchange_tag varchar(3),
    payments_tag varchar(5),
    PRIMARY KEY (id),
    FOREIGN KEY (id) REFERENCES cryptodata(Id)
 ) ;

# Insert data into cryptotag table
INSERT into cryptotag (id, mineable_tag, exchange_tag, payments_tag)
SELECT id, mineable_tag, exchange_tag, payments_tag FROM crypto_main;

# Retrieve data from cryptotag table
SELECT * FROM cryptotag;

# Analysis
#--------------------------------------------------------------------------------------------------------------------------------------------
# Top 10 coin market cap ranked cryptocurrencies
SELECT cmc_rank "CMC Rank", cmc_name "Name of the cryptocurrency", price, market_cap "Market Capitalization"
FROM cryptodata
ORDER BY cmc_rank ASC
LIMIT 10;

# Highest active trading pairs for cryptocurrencies.
SELECT cmc_name "Name of the cryptocurrency", num_market_pairs "Active trading pairs"
FROM cryptodata
ORDER BY num_market_pairs DESC
LIMIT 3;

# Central tendencies of price for cryptocurrience
SELECT 
	min(price) "Minimum Price", 
	round(max(price),2) "Maximum Price", 
    round(avg(price),2) "Average Price",
    round(stddev(price),2) "Standard Deviation of Price", 
    round(variance(price),2) "Variance of Price"
FROM cryptodata;

# Central tendencies of volume for cryptocurrience
SELECT 
	min(volume_24h) "Minimum Volume", 
	round(max(volume_24h),2) "Maximum Volume", 
    round(avg(volume_24h),2) "Average Volume",
    round(stddev(volume_24h),2) "Standard Deviation of Volume", 
    round(variance(volume_24h),2) "Variance of Volume"
FROM cryptovolch ;

# Currencies that would be suitable for mining
SELECT d.id, cmc_name, price, round(max_supply-total_supply,2) "Mineable Units"
FROM cryptodata d
INNER JOIN cryptotag t
ON d.id = t.id
INNER JOIN cryptosupply s
ON d.id = s.id
WHERE mineable_tag='Yes' AND max_supply > 0;

# Coins that are almost completely mined
SELECT d.id, cmc_name, price, round(max_supply-total_supply,2) "Mineable Units"
FROM cryptodata d
INNER JOIN cryptotag t
ON d.id = t.id
INNER JOIN cryptosupply s
ON d.id = s.id
WHERE mineable_tag='Yes' AND max_supply > 0
ORDER BY abs(max_supply-total_supply) ASC;

# Currencies that would be suitable for exchange and payments
SELECT d.id, cmc_name, price, exchange_tag, payments_tag
FROM cryptodata d
INNER JOIN cryptotag t
ON d.id = t.id
WHERE exchange_tag='Yes' OR payments_tag='Yes' AND price BETWEEN 0 AND 10000;

# Stable coins in a given price range
SELECT d.id, cmc_name, price, abs(percent_change_30d) "Percentage change"
FROM cryptodata d
INNER JOIN cryptopctch p
ON d.id=p.id
WHERE price BETWEEN 0 AND 1000
ORDER BY abs(percent_change_30d) ASC;

# Volatile coins in a given price range
SELECT d.id, cmc_name, price, abs(percent_change_30d) "Percentage change"
FROM cryptodata d
INNER JOIN cryptopctch p
ON d.id=p.id
WHERE abs(percent_change_30d) < 1000 AND price BETWEEN 0 AND 1000
ORDER BY abs(percent_change_30d) DESC;

# Top Drops in 90 days
SELECT d.id, cmc_name
FROM cryptodata d
INNER JOIN cryptopctch p
ON d.id=p.id
ORDER BY abs(percent_change_90d) DESC;

# Top Rises in 90 days
SELECT d.id, cmc_name
FROM cryptodata d
INNER JOIN cryptopctch p
ON d.id=p.id
ORDER BY abs(percent_change_90d) ASC;



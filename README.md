# CS623-Project-Aksh-Clifton
CS623-Project

Create Local Database

CREATE DATABASE Production;


CREATE TABLE Product (
    prod  VARCHAR(10),
    pname VARCHAR(50),
    price NUMERIC(10,2)
);

CREATE TABLE Depot (
    dep    VARCHAR(10),
    addr   VARCHAR(100),
    volume INTEGER
);

CREATE TABLE Stock (
    prod     VARCHAR(10),
    dep      VARCHAR(10),
    quantity INTEGER
);

ALTER TABLE Product
ADD CONSTRAINT pk_product PRIMARY KEY (prod);

ALTER TABLE Depot
ADD CONSTRAINT pk_depot PRIMARY KEY (dep);

ALTER TABLE Stock
ADD CONSTRAINT pk_stock PRIMARY KEY (prod, dep);

ALTER TABLE Stock
ADD CONSTRAINT fk_stock_product
FOREIGN KEY (prod) REFERENCES Product(prod);

ALTER TABLE Stock
ADD CONSTRAINT fk_stock_depot
FOREIGN KEY (dep) REFERENCES Depot(dep);


Insert Data to local database

INSERT INTO Product (prod, pname, price) VALUES
('p1', 'tape', 2.5),
('p2', 'tv', 250),
('p3', 'vcr', 80);

INSERT INTO Depot (dep, addr, volume) VALUES
('d1', 'New York', 9000),
('d2', 'Syracuse', 6000),
('d4', 'New York', 2000);

INSERT INTO Stock (prod, dep, quantity) VALUES
('p1', 'd1', 1000),
('p1', 'd2', -100),
('p1', 'd4', 1200),
('p3', 'd1', 3000),
('p3', 'd4', 2000),
('p2', 'd4', 1500),
('p2', 'd1', -400),
('p2', 'd2', 2000);
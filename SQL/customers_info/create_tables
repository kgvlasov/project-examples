CREATE TABLE tblClients (
  	ClientID int,
    Status varchar(255));

CREATE TABLE tblTestClients (
  	ClientID int);

CREATE TABLE tblClientBalanceOperation (
  	BalanceOperationID int,
	ClientID int,
	Amount int,
	SignOfPayment boolean,
	OperationTime date);

CREATE TABLE tblOnlineSessions_mini (
  	ClientID int,
    OnlineTime date,
  	OfflineTime date);

INSERT INTO tblClients VALUES (0,'Active'),(1,'Updated by client'),(2,'Deleted'),(3,'Passive'),(4,'Active'),(5,'Active');
INSERT INTO tblTestClients VALUES (5),(6);

INSERT INTO tblClientBalanceOperation VALUES (0,0,22,True,'2021-01-01'),(1,0,15,True,'2021-02-04'),(2,0,120,False,'2021-02-05'),(3,1,16,True,'2021-01-03'),(4,4,59,True,'2021-01-07'),(5,3,20,False,'2021-01-05'),(6,5,5,True,'2021-01-07'),(7,5,15,True,'2021-01-08'),(8,3,5,False,'2021-02-05'),(9,0,20,True,'2021-03-03'),(10,0,15,True,'2021-03-04'),(11,0,120,False,'2021-03-05'),(12,1,20,True,'2021-03-03'),(13,4,59,True,'2021-03-07'),(14,3,20,False,'2021-03-04'),(15,5,5,True,'2021-03-07'),(16,5,15,True,'2021-03-08'),(17,3,5,False,'2021-03-05'),(18,0,20,True,'2021-04-03'),(19,0,15,True,'2021-04-04'),(20,0,120,False,'2021-04-05'),(21,1,10,True,'2021-04-03'),(22,4,59,True,'2021-04-07'),(23,3,20,False,'2021-04-02'),(24,5,5,True,'2021-04-07'),(25,5,15,True,'2021-04-08'),(26,3,5,False,'2021-04-05');


WITH q1 AS (
 SELECT 
	ClientID AS ClientID,
   	MIN(OperationTime) AS second_purchase_time
FROM 
 	tblClientBalanceOperation
WHERE
	OperationTime NOT IN 
   		(
    		SELECT 
       			MIN(OperationTime)
	 		FROM tblClientBalanceOperation
          	GROUP BY
    			 ClientID
   		)      
GROUP BY
     ClientID
 ),
 
 
q2 AS (
  SELECT
  	tblClientBalanceOperation.ClientID,
  	Amount AS second_purchase_amount
  FROM
  	tblClientBalanceOperation
  	LEFT JOIN q1 ON q1.ClientID = tblClientBalanceOperation.ClientID
  WHERE 
  	q1.ClientID = tblClientBalanceOperation.ClientID
  AND tblClientBalanceOperation.OperationTime=q1.second_purchase_time
  ),
  
  
q3 AS (
    SELECT
    	ClientID,
    	first_value(tblClientBalanceOperation.Amount) OVER (PARTITION BY tblClientBalanceOperation.ClientID ORDER BY  tblClientBalanceOperation.OperationTime) AS first_purchase_amount
    FROM
    	tblClientBalanceOperation
    ),
    
    
q4 AS (
  SELECT 
  	ClientID,
  	MIN(OperationTime) AS first_purchase_date
  FROM
  	tblClientBalanceOperation
  GROUP BY
  	ClientID
  ),
  
  
q5 AS (
  SELECT 
  	tblClientBalanceOperation.ClientID,
  	SUM(tblClientBalanceOperation.Amount) OVER (PARTITION BY tblClientBalanceOperation.ClientID) AS cumulative_first_month_purchase
  FROM
  	tblClientBalanceOperation
  	LEFT JOIN q4 ON q4.ClientID = tblClientBalanceOperation.ClientID
	WHERE
  		tblClientBalanceOperation.OperationTime <= q4.first_purchase_date+30
  ),  
  
  
q6 AS (
  SELECT
  	ClientID,
  	tblClientBalanceOperation.OperationTime - LAG (tblClientBalanceOperation.OperationTime) OVER (PARTITION BY tblClientBalanceOperation.ClientID ORDER BY tblClientBalanceOperation.OperationTime) AS Average_days_between_purchase
   FROM  
      tblClientBalanceOperation                                                  
  )


SELECT
    tblClientBalanceOperation.ClientID,
    MIN(q4.first_purchase_date) AS first_purchase_date,
    MIN(q3.first_purchase_amount) AS first_purchase_amount,
    MIN(q1.second_purchase_time) AS second_purchase_time,
    MIN(q2.second_purchase_amount) AS second_purchase_amount,
    MAX(OperationTime) AS last_purchase_date,
    MAX(second_purchase_time) - MIN(OperationTime) AS days_between_purchase,
    MIN(q5.cumulative_first_month_purchase) AS cumulative_first_month_purchase,
    ROUND(AVG(q6.Average_days_between_purchase),0) AS Average_days_between_purchase
FROM tblClientBalanceOperation
	LEFT JOIN q1 ON q1.ClientID = tblClientBalanceOperation.ClientID
	LEFT JOIN q2 ON q2.ClientID = tblClientBalanceOperation.ClientID
    LEFT JOIN q3 ON q3.ClientID = tblClientBalanceOperation.ClientID
    LEFT JOIN q4 ON q4.ClientID = tblClientBalanceOperation.ClientID
    LEFT JOIN q5 ON q5.ClientID = tblClientBalanceOperation.ClientID
    LEFT JOIN q6 ON q6.ClientID = tblClientBalanceOperation.ClientID
WHERE
	tblClientBalanceOperation.ClientID NOT IN (SELECT ClientID FROM tblTestClients)        
GROUP BY
	tblClientBalanceOperation.ClientID

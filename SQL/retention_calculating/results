SELECT 
    meta.cohort_name AS date_of_installation_cohort,
    c_ret.cohort_ages AS cohort_ages_days,
    c_ret.retention/meta.num_users AS retention_rate
FROM
    (SELECT
      ADDDATE(installed_at,-DAY(installed_at)+1) AS cohort_name,
      COUNT(DISTINCT user.user_id) AS num_users
    FROM user  
     GROUP BY 1) meta
     
    LEFT JOIN 
     
    (SELECT 
      ADDDATE(installed_at,-DAY(installed_at)+1) AS cohort_name,
      datediff(cs.created_at, installed_at) AS cohort_ages,
      COUNT(DISTINCT user.user_id) AS retention
    FROM user
      LEFT JOIN client_session cs ON cs.user_id=user.user_id
    WHERE 1=1
      AND datediff(cs.created_at, installed_at) IN (1,3,7)        
    GROUP BY 1, 2) c_ret ON meta.cohort_name = c_ret.cohort_name
WHERE
  meta.cohort_name > '2019-12-31'
ORDER BY 1,2

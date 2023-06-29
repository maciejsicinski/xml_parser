import sqlglot
import sqlglot.expressions as exp
from sqlglot import parse_one, exp

def find_columns(query):
    column_names = []
    for expression in sqlglot.parse_one(query).find(exp.Select).args["expressions"]:
        column_names.append(expression.alias_or_name)
    return column_names

query = """
SELECT
     samh.Access_Method
    ,samh.Subscription_Id
    ,samh.Account_Id
    ,samh.Subscr_Acc_Method_Start_Dttm
    ,samh.Subscr_Acc_Method_End_Dttm
    ,COALESCE(MIN(samh.End_Dttm) OVER (PARTITION BY samh.Access_Method    ORDER BY samh.ORD_ASC   ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)   + INTERVAL '1' SECOND, samh.Start_Dttm) AS Start_Dttm 
    ,samh.End_Dttm
    ,samh.Imsi
    ,samh.Main_Access_Method_Ind
    ,sub.Subscription_Type_Id
    ,sub.Serial_Number
    ,sub.Subscription_Status_Id
    ,sub.Activation_Dttm
    ,sub.Deactivation_Dttm
    ,sty.Subscription_Type_Group_Id

FROM
    (
    
    
     SELECT 
          Access_Method
         ,Subscr_Acc_Method_Start_Dttm
         ,Subscr_Acc_Method_End_Dttm
         ,Subscription_Id
         ,Account_Id
         ,CASE
             WHEN (CASE 
                      WHEN Next_Sub_Acc_Method_Start_Dttm < Subscr_Acc_Method_End_Dttm 
                      THEN INTERVAL '0' DAY
                      ELSE ((CAST(Next_Sub_Acc_Method_Start_Dttm AS DATE) - CAST(Subscr_Acc_Method_End_Dttm AS DATE)) DAY(4)) 
                   END) = 0
                   THEN (EXTRACT(HOUR FROM Next_Sub_Acc_Method_Start_Dttm) * 3600 + EXTRACT(MINUTE FROM Next_Sub_Acc_Method_Start_Dttm) * 60 
                        + EXTRACT(SECOND FROM Next_Sub_Acc_Method_Start_Dttm)) - (EXTRACT(HOUR FROM Subscr_Acc_Method_End_Dttm) * 3600
                        + EXTRACT(MINUTE FROM Subscr_Acc_Method_End_Dttm) * 60 + EXTRACT(SECOND FROM Subscr_Acc_Method_End_Dttm)) - 1
                   ELSE ((EXTRACT(HOUR FROM Next_Sub_Acc_Method_Start_Dttm) * 3600 + EXTRACT(MINUTE FROM Next_Sub_Acc_Method_Start_Dttm) * 60 
                          + EXTRACT(SECOND FROM Next_Sub_Acc_Method_Start_Dttm)) + 84600
                          - (EXTRACT(HOUR FROM Subscr_Acc_Method_End_Dttm) * 3600
                          + EXTRACT(MINUTE FROM Subscr_Acc_Method_End_Dttm) * 60
                          + EXTRACT(SECOND FROM Subscr_Acc_Method_End_Dttm))
                        ) / 2
          END AS Sec_Mean_Diff
         ,Subscr_Acc_Method_End_Dttm + 
         
          (CASE
              WHEN Next_Sub_Acc_Method_Start_Dttm < Subscr_Acc_Method_End_Dttm
              THEN INTERVAL '0' DAY 
              ELSE ((CAST(Next_Sub_Acc_Method_Start_Dttm AS DATE) - CAST(Subscr_Acc_Method_End_Dttm AS DATE)) DAY(4)) 
           END / 2)
           + CAST(Sec_Mean_Diff/3600 AS INTERVAL HOUR) 
           + CAST(Sec_Mean_Diff MOD 3600 / 60 AS INTERVAL MINUTE)
           + CAST((Sec_Mean_Diff MOD 3600) MOD 60 AS INTERVAL SECOND) AS Mean_End_Ts
        
         ,CASE
             WHEN ORD_ASC = 1 
             THEN CAST('1900-01-01 00:00:00' AS TIMESTAMP(0)) 
             WHEN Subscr_Acc_Method_Start_Dttm = Prev_Sub_Acc_Method_Start_Dttm
             THEN Prev_Sub_Acc_Method_End_Dttm
             ELSE Subscr_Acc_Method_Start_Dttm 
          END AS Start_Dttm
        
         ,CASE
             WHEN ORD_DESC = 1 
             THEN CAST('9999-12-31 00:00:00' AS TIMESTAMP(0)) 
             ELSE CASE
                     WHEN Subscr_Acc_Method_End_Dttm > Next_Sub_Acc_Method_Start_Dttm AND Subscr_Acc_Method_Start_Dttm < Next_Sub_Acc_Method_Start_Dttm
                     THEN Next_Sub_Acc_Method_Start_Dttm
                     WHEN Subscr_Acc_Method_End_Dttm > Next_Sub_Acc_Method_Start_Dttm AND Subscr_Acc_Method_Start_Dttm = Next_Sub_Acc_Method_Start_Dttm
                     THEN Subscr_Acc_Method_End_Dttm                     
                     ELSE Mean_End_Ts
                  END
          END AS End_Dttm
         ,SUM(1) OVER (PARTITION BY Access_Method                ORDER BY Subscr_Acc_Method_Start_Dttm, Subscr_Acc_Method_End_Dttm, Main_Access_Method_Ind , Subscription_Id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ORD_ASC
         ,SUM(1) OVER (PARTITION BY Access_Method                ORDER BY Subscr_Acc_Method_Start_Dttm DESC, Subscr_Acc_Method_End_Dttm DESC, Main_Access_Method_Ind  DESC, Subscription_Id DESC   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ORD_DESC
         ,MIN(Subscr_Acc_Method_Start_Dttm) OVER (PARTITION BY Access_Method     ORDER BY Subscr_Acc_Method_Start_Dttm, Subscr_Acc_Method_End_Dttm, Main_Access_Method_Ind , Subscription_Id  ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) Next_Sub_Acc_Method_Start_Dttm
         ,MIN(Subscr_Acc_Method_End_Dttm) OVER (PARTITION BY Access_Method     ORDER BY Subscr_Acc_Method_Start_Dttm, Subscr_Acc_Method_End_Dttm, Main_Access_Method_Ind , Subscription_Id  ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) Prev_Sub_Acc_Method_End_Dttm
         ,MIN(Subscr_Acc_Method_Start_Dttm) OVER (PARTITION BY Access_Method     ORDER BY Subscr_Acc_Method_Start_Dttm, Subscr_Acc_Method_End_Dttm, Main_Access_Method_Ind , Subscription_Id  ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) Prev_Sub_Acc_Method_Start_Dttm                                    
         ,Imsi
         ,Main_Access_Method_Ind

     FROM
          vDW_Subscription_Acc_Method_Hi
     WHERE
          EXTRACT(YEAR FROM Subscr_Acc_Method_Start_Dttm) < 2020
    AND
        EXTRACT(YEAR FROM Subscr_Acc_Method_End_Dttm) > 2004
    AND
        Subscr_Acc_Method_Start_Dttm < Subscr_Acc_Method_End_Dttm
        
    ) samh
INNER JOIN
     vDW_Subscription sub
ON   samh.Subscription_Id = sub.Subscription_Id
INNER JOIN
     vDW_Subscription_Type sty
ON   sub.Subscription_Type_Id = sty.Subscription_Type_Id
"""

print(find_columns(query))
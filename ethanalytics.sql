with tokens as 
(
SELECT A.address,coalesce(A.name,B.name) as name,coalesce(A.symbol,B.symbol) as symbol ,safe_Cast(total_supply as float64) as Max_Supply,coalesce(A.decimals,B.decimals) as decimals    FROM `bigquery-public-data.crypto_ethereum.tokens`  A
left join `bigquery-public-data.crypto_ethereum.amended_tokens`  B on A.address  = B.address
-- where address =  '0xdac17f958d2ee523a2206206994597c13d831ec7'
)
 
 
 
 
 
,transactions as 
(
  Select * from  `bigquery-public-data.crypto_ethereum.transactions` 
)
,logs as 
(
  Select * from  `bigquery-public-data.crypto_ethereum.logs` 
)
  ,transactionsxtokens as 
(
 Select distinct nft.address ,nft.name,symbol,Max_Supply,decimals
    from transactions
     tx join logs log on tx.hash = log.transaction_hash
  inner join tokens nft on log.address = nft.address
  where date(tx.block_timestamp) >= date_add(current_date(), interval -31 day) 
 
)
 
, transfers AS (
    SELECT
    token_address,
    from_address AS address,from_address,to_address,
    -safe_cast(value as numeric) AS amount,
    block_timestamp
     FROM  `bigquery-public-data.crypto_ethereum.token_transfers` tr
    -- tx.block_timestamp >= (datetime_add(datetime_trunc( CURRENT_TIMESTAMP,hour) ,interval -25 hour)
     
    --  where date(block_timestamp) >= date_add(current_date(), interval -1 day)
UNION ALL
 
    SELECT
  token_address,
    to_address AS address,from_address,to_address,
    safe_cast(value as numeric)  AS amount,
    block_timestamp
     FROM   `bigquery-public-data.crypto_ethereum.token_transfers`
    --  where date(block_timestamp) >= date_add(current_date(), interval -1 day)
     
)
 ,top_10_holders as 
 (Select token_address,sum(amount) as Top_address_supply from
 
 (Select *,row_number() over (partition by token_address order by  amount desc) as Rnum from
 
    (Select token_address,address,sum(amount/1000000000000000000) as amount 
 from transfers
 group by 1,2)
 )
 where Rnum = 1
 group by 1
 )
,transfers2 as 
(
     SELECT
    token_address,
    from_address,to_address,
    safe_cast(value as numeric) AS amount,
    block_timestamp
     FROM  `bigquery-public-data.crypto_ethereum.token_transfers` tr
)
,balancea as 
(
  Select * from  `bigquery-public-data.crypto_ethereum.balances` 
  where eth_balance  > 0
)
,burnsuppy as 
(
  select A.token_address,sum(case when to_address in ('0x0000000000000000000000000000000000000000','0x000000000000000000000000000000000000dead',
  '0xdead000000000000000042069420694206942069',
'0x0000000000000000000000000000000000000000'
  ) then safe_cast(amount as numeric)/1000000000000000000 end) as Burn_supply
,count(distinct case when to_address <> '0x000000000000000000000000000000000000dead' then address end) as Holder1 from transfers2
   A
    inner JOIN  tokens B  ON A.token_address = B.address
    group by 1
)
,holders as 
(select token_address,count(distinct address ) as Holders,max(amount) as Max_holder_amount 
 
 
from
(
    SELECT 
    token_address,A.address ,amount ,to_address
    FROM transfers A
    inner JOIN  tokens B  ON A.token_address = B.address
     inner join balancea C on A.address = C.address
)
where amount > 0
  GROUP BY 1
) 
,transactions_agg as 
(
 Select log.address , Avg(tx.value) /1000000000000000000   as Average_transaction ,
 count(1) as Num_of_Transactions,countif(tx.block_timestamp >= (datetime_add(datetime_trunc( CURRENT_TIMESTAMP,hour) ,interval -25 hour))
  AND tx.block_timestamp < datetime_trunc( CURRENT_TIMESTAMP,hour) ) as Last24h_Num_transactions,
    SUM(tx.value) / 1000000000000000000 as  Total_transactions_volumne,
    sum(case when tx.block_timestamp >= (datetime_add(datetime_trunc( CURRENT_TIMESTAMP,hour) ,interval -25 hour))
  AND tx.block_timestamp < datetime_trunc( CURRENT_TIMESTAMP,hour) then tx.value end)/1000000000000000000 as Last24h_transaction_value,
  
    from transactions
     tx join logs log on tx.hash = log.transaction_hash
  inner join tokens nft on log.address = nft.address
  group by 1
)
 
 
,holders_max_min as 
(select token_address,date ,count(distinct address ) as Holders,sum(amount/1000000000000000000) as Amount 
 
 
from
(
    SELECT 
    token_address,A.address ,amount , date(block_timestamp) as date
    FROM transfers A
    inner JOIN  tokens B  ON A.token_address = B.address
        inner join balancea C on A.address = C.address
 
)
where amount > 0
  GROUP BY 1,2
) 
,Max_holder_info as 
(Select *except(Rank) from
(Select token_address, date as Max_Holders_Date_reached,Holders as Max_holders,Amount as Max_Holder_Value,row_number() over(partition by token_address order by Holders desc ) as Rank from holders_max_min
)
where Rank =1
)
 
,Min_holder as 
(Select *except(Rank) from
(Select token_address,date as Min_Holders_Date_reached,Holders as Min_holders,Amount as Min_Holder_Value,row_number() over(partition by token_address order by Holders ) as Rank from holders_max_min
)
where Rank =1
)
 
 
 
Select A.*except(Max_Supply),Max_Supply,(Max_Supply - Burn_supply ) as Total_Supply,Top_address_supply,(Max_Supply - Top_address_supply) as Current_est_Supply,
 
 B.Holders , Num_of_Transactions ,Total_transactions_volumne,Last24h_transaction_value,Burn_supply,Holder1
,Last24h_Num_transactions,E.*except(token_address),F.*except(token_address),
case when Max_Holders_Date_reached > Min_Holders_Date_reached  then 'True' else 'False'
end as Is_Min_date_occure_first
 
from transactionsxtokens A
left join holders B on A.address = B.token_address
left join transactions_agg C on A.address = C.address
left join burnsuppy D on A.address = D.token_address
left join Max_holder_info E on A.address = E.token_address
left join Min_holder F on A.address = F.token_address 
left join top_10_holders G on A.address = G.token_address 
where holders >= 15000
order by Holders desc
 
 

--For this take home, I am confined to using SQL only. I use Python often in Databricks, however, we get charged on a by use bases and I don't
--want to cross any professional lines. Same thing goes for our visualization software Tableau. I will talk through the portions where I would use
--Python in greater detail. I often use python and SQL in tandem.

--Are there any data quality issues present?

--Check for completeness
--For the time being, I am going to assume null or blank values are OK for non-key columns

select count(*) from users_data
where id IS NULL

--OUTPUT is 0 records.

select count(*) from product_data
where barcode IS NULL

--There are 4025 of 845552 rows that have NULL values for barcode (.5% approx), which is what we use to connect to transactions.
--We can't connect those to transactions

select count(*) from transactions_data
where user_id IS NULL

select count(*) from transactions_data
where barcode IS NULL

--0 transaction.user_id is null, 2807 transaction.barcode IS NULL.
--I did a simple select * where those barcode values are null, nothing odd about the data. Just no value. Lets get rid of them for this analysis.

delete from product_data
where barcode is null

delete from transactions_data
where barcode is null

--after seeing the rest of the data, I am satisfied with the remaining null values

--let's see whats going on with 'final quantity' column. It's got both text and numerical data

select distinct quantity from transactions_data
--lucky for me there are only a few values, and I see only one issue with text representing a number. if this
--had not been the case, I would have to use regex and say something like select * where final_quanity REGEXP '[A-Za-z]'
--to find all the values where there are letters present in what should be a numerical field

UPDATE transactions_data
SET quantity = '0'
WHERE quantity = 'zero'

--Because im working in SQL, and my IDE isnt very user friendly, i have to create a new table with the right datatypes now
--I brought everything in as text to ensure I didnt lose anything due to datatype issues
create table transactions_data_new (
    receipt_id varchar,
    purchase_date date,
    scan_date datetime,
    store_name varchar,
    user_id varchar,
    barcode varchar,
    quantity double,
    sale double
)

insert into transactions_data_new (receipt_id, purchase_date, scan_date, store_name, user_id, barcode, quantity, sale)
select receipt_id, purchase_date, scan_date, store_name, user_id, barcode, quantity, sale from transactions_data

--now i can use those two columns as numbers without casting
--there arent a lot of quantity types, so i can analyze just by doing distinct counts

select distinct quantity, count(*) from transactions_data_new
group by quantity
order by count(*) desc

--42,651 of the 44,238 are either quants of 1 or 0. thats 96.4%.
--we have decimals in the quantity as well, maybe that might mean weight of an item? Unsure at this point.

select
CASE
        WHEN category_4 IS NOT NULL THEN category_4
        WHEN category_3 IS NOT NULL THEN category_3
        WHEN category_2 IS NOT NULL THEN category_2
        WHEN category_1 IS NOT NULL THEN category_1
        ELSE NULL
    END AS product,
    count(*)
from transactions_data_new a join product_data b on a.barcode = b.barcode
where quantity <> round(quantity, 0)
group by
CASE
        WHEN category_4 IS NOT NULL THEN category_4
        WHEN category_3 IS NOT NULL THEN category_3
        WHEN category_2 IS NOT NULL THEN category_2
        WHEN category_1 IS NOT NULL THEN category_1
        ELSE NULL
END

--some items you might buy by weight, others it doesnt make sense... I would probably raise my hand about these:
--Confection Candy
--Peanuts
--Breakfast Cereal
--Crackers
--Potato Chips
--Regular Cola
--Vitamins & Herbal Supplements
--Chocolate Candy
--Dried Fruit
--Ready-to-Eat Gelatin
--Tortilla Chips

--lets look at the actual $sales amounts and see if these are looking good

select count(*) from transactions_data_new
where quantity != 0
and trim(sale) != ''

--22,119 rows have quantity and sales amounts out of 44,238. Thats exactly twice as much. Something is going on here
-- lets explore a specific transaction

select * from transactions_data_new
where receipt_id = '00017e0a-7851-42fb-bfab-0baa96e23586'

--one row has a sales amount, the other doesnt.

select rows, count(rows) from (select receipt_id, count(*) as rows
               from transactions_data_new
               group by receipt_id)
group by rows

-- Here are the breakdowns of receipt_ids by count
--2,21195
--4,416
--6,22
--8,5
--12,1

select receipt_id from (select receipt_id, count(*) as rows
               from transactions_data_new
               group by receipt_id)
where rows >= 4

select * from transactions_data_new
where receipt_id = '007d3232-3990-497f-a081-549e9e7a478b'

--exploring a receipt that has 4 entries, looks like its just messy data.
--moving forward, i am going to get rid of the rows that have 0 quantity
--if we have dupes I will account for that as to not inflate sales

--at this point i would definitely do more analysis on data health, but for a take home assignment I think this is a
--fair display of how I would approach.

select * from transactions_data_new a join users_data b on trim(a.user_id) = trim(b.id) limit 20

--we're looking for 22,119 transactions, lets make sure we dont cause issues with joins

--DISCLAIMER!!! My IDE i'm using is slugging on these queries, so I am just going to write what
--would solve the problems, I'm not sure of the answers..

--What are the top 5 brands by receipts scanned among users 21 and over?

select brand, count(distinct receipt_id) from
                     (select brand,
                              category_1,
                              category_2,
                              category_3,
                              category_4,
                              sale,
                              date('now') - date(b.birth_date) as age, --normally I would wrap in a "year()" but this actually worked to gather year difference
                              birth_date, -- to compare age, ensure its accurate
                              a.receipt_id
                       from transactions_data_new a
                                left join users_data b on trim(a.user_id) = trim(b.id)
                                left join product_data c on trim(a.barcode) = trim(c.barcode)
                       where a.quantity != 0
                        and trim(sale) != ''
                        and c.brand is not null
                        and b.birth_date is not null
)
where age >= 21
group by brand
order by count(distinct receipt_id) desc
limit 1

--this query would show you the top brand by receipt count for customers over 21



--Who are Fetch’s power users?
--what does it mean to be a power user? lets find out


select user_id, accountage/transactionamount
from (select user_id,
             JULIANDAY('now') - JULIANDAY(b.created_date) AS accountage,
             count(distinct receipt_id)                   AS transactionamount
      from transactions_data_new a
               join users_data b on a.user_id = b.id
      group by user_id,
               JULIANDAY('now') - JULIANDAY(b.created_date)
      order by count(distinct receipt_id) desc)
order by accountage/transactionamount

--user_id 6682b24786cc41b000ce5e77 is a good contender for being our top power user
--in this set of data. I created a subquery which calculates the amount of transactions
--(I specifically chose this as a indicator of usage, not sales. we dont want to award
--a large purchase as someone who is a power user) along with the age of account
--I then calculated the avg amount of days between transactions, with that users
--avg being about 107 days between purchases which is the best ratio.



SELECT strftime('%Y', created_date) AS year,
       COUNT(DISTINCT id) AS user_count
FROM users_data
GROUP BY strftime('%Y', created_date)
ORDER BY strftime('%Y', created_date)

--this table is showing each year, and its count of 'created_date'. I would normally use a
-- LAG() function to determine YoY growth, but its not supported in sqlite. so I have to
--self join

WITH yearly_data AS (
    SELECT CAST(strftime('%Y', created_date) AS INTEGER) AS year,
           COUNT(DISTINCT id) AS user_count
    FROM users_data
    GROUP BY year
)
SELECT
    y1.year,
    y1.user_count,
    y2.user_count AS previous_year_value,
    ROUND((CAST(y1.user_count AS FLOAT) - y2.user_count) / y2.user_count * 100, 2) AS yoy_growth_percentage
FROM yearly_data y1
LEFT JOIN yearly_data y2
    ON y1.year = y2.year + 1
ORDER BY y1.year;







--Q1
select country_code, count (*) from dim_store_details group by country_code	order by count (*) desc;
--Q2
select locality, count (*) from dim_store_details group by locality	ORDER BY COUNT(*) DESC;
--Q3
select round(sum(orders_table.product_quantity*dim_products.product_price)) as total_revenue,
dim_date_times.month
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
group by dim_date_times.month
ORDER BY sum(orders_table.product_quantity*dim_products.product_price) DESC;

--Q4
select 	count (orders_table.product_quantity) as numbers_of_sales,sum(orders_table.product_quantity) as product_quantity_count,
	case 
		when dim_store_details.store_code = 'WEB-1388012W' then 'Web'
		else 'Offline'
		end as product_location
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by product_location
ORDER BY sum(orders_table.product_quantity) ASC;

--Q5
select 	dim_store_details.store_type, 
sum (orders_table.product_quantity*dim_products.product_price) as revenue,
sum(100.0*orders_table.product_quantity*dim_products.product_price)/(sum(sum(orders_table.product_quantity*dim_products.product_price)) over ()) AS percentage_total
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by dim_store_details.store_type
ORDER BY percentage_total DESC;

--Q6
select round(sum(orders_table.product_quantity*dim_products.product_price)) as revenue, dim_date_times.year,dim_date_times.month
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by dim_date_times.month,dim_date_times.year
ORDER BY sum(orders_table.product_quantity*dim_products.product_price)  DESC;

--Q7
select sum(dim_store_details.staff_numbers) as total_staff_numbers, dim_store_details.country_code
from dim_store_details
group by dim_store_details.country_code
order by total_staff_numbers desc;

--Q8
select round(count(orders_table.date_uuid) , 2) as total_sales	, 
dim_store_details.store_type, 
dim_store_details.country_code
from orders_table
join dim_date_times on  orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on  orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
where dim_store_details.country_code = 'DE'
group by dim_store_details.store_type,dim_store_details.country_code
ORDER BY total_sales DESC;

--Q9

select  dim_date_times.year,
TO_CHAR(
        'HH12:MI:SS'
    ) timestamp
		 		  
from dim_date_times
group by dim_date_times.year
order by avg(dim_date_times.timestamp) desc;

select * from dim_date_times




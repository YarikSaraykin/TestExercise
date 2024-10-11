
WITH MonthSales AS 
(
    SELECT 
		g.gr_name AS group_name,
        YEAR(ord_datetime) AS year,
        MONTH(ord_datetime) AS month,
        COUNT(o.ord_id) AS sold_tests

    FROM med.Orders o

    INNER JOIN med.Analysis a 
		ON o.ord_an = a.an_id
    INNER JOIN med.Groups g 
		ON a.an_group = g.gr_id

    GROUP BY g.gr_name, YEAR(o.ord_datetime), MONTH(o.ord_datetime)
)

SELECT 
	year,
	month,
	group_name,
	SUM(sold_tests) OVER (PARTITION BY group_name, year ORDER BY year, month) AS sold_tests

FROM MonthSales

ORDER BY year, month, group_name 







SELECT 
	a.an_name,
	a.an_cost

FROM med.Analysis a
INNER JOIN med.Orders o
	ON a.an_id = o.ord_an
WHERE o.ord_datetime BETWEEN '20200205' AND CAST(DATEADD(second, -1, DATEADD(day, 7, '20200205')) AS DATETIME);
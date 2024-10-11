WITH MonthsList AS 
(
    SELECT TOP (60)
        DATEADD(MONTH, -ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + 1, CAST(GETDATE() AS DATE)) AS month
    FROM sys.all_objects
),

Months AS
(
	SELECT 
		CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, month), 0) as DATE) as month
	FROM MonthsList
),

IDs AS
(
	SELECT DISTINCT 
		CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, DT), 0) as DATE) as month,
		ClientID

	FROM dbo.Orders
	WHERE ClientID IS NOT NULL
),

MonthsIDs AS
(
	SELECT 
		month,
		STRING_AGG(ClientID, ',') as ids

	FROM IDs 

	GROUP BY month
),

SumPerMonth AS
(
	SELECT
		CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, o.DT), 0) as DATE) as month,
		SUM(o.Summ) as monthly_sum

	FROM dbo.Orders o 
	WHERE o.ClientID IS NOT NULL
	GROUP BY DATEADD(MONTH, DATEDIFF(MONTH, 0, DT), 0)
),

AllData AS 
(
	SELECT 
		ms.month,
		ms.monthly_sum,
		mID.ids

	FROM SumPerMonth ms
	INNER JOIN MonthsIDs mID
		ON ms.month = mID.month
)

INSERT INTO dbo.Orders (DT, ClientID, Summ, comment)

	SELECT
		DATEADD(MONTH, 1, m.month) AS DT,
		NULL AS ClientID,
		ISNULL(ad.monthly_sum, 0) AS monthly_sum,
		ISNULL(ad.ids, '') AS ids

	FROM Months m

	LEFT JOIN AllData ad
		ON m.month = ad.month

	WHERE m.month >= (SELECT min(month) FROM AllData)
			AND m.month <= (SELECT max(month) FROM AllData)

	ORDER BY m.month;

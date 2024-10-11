
WITH GroupedSamples AS
(
	SELECT 
		client_id, 
		client_name, 
		client_balance_date, 
		client_balance_value,

		ROW_NUMBER() OVER 
		(
			PARTITION BY client_id, client_name, client_balance_date, client_balance_value 
			ORDER BY client_id
		) AS sample

	FROM dbo.ClientBalance
)

DELETE FROM GroupedSamples
WHERE sample > 1;
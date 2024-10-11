
DROP TABLE IF EXISTS dbo.Orders;

CREATE TABLE dbo.Orders
(
	id INT PRIMARY KEY NOT NULL IDENTITY(1,1),
	DT DATETIME NOT NULL,
	ClientID INT NULL, 
	Summ DECIMAL(10,2) NOT NULL,
	comment VARCHAR(MAX) NULL
);

INSERT INTO dbo.Orders (DT, ClientID, Summ, comment)
VALUES 
('20200101 01:00', 1, 100, NULL),
('20200102 01:00', 2, 200, NULL),
('20200103 01:00', 2, 300, NULL),
('20200112 01:00', 3, 400, NULL),
('20200115 01:00', 3, 500, NULL),
('20200202 01:00', 1, 100, NULL),
('20200212 02:00', 2, 200, NULL),
('20200220 06:00', 3, 300, NULL),
('20200225 04:00', 1, 400, NULL),
('20200320 01:00', 1, 100, NULL),
('20200331 01:00', 1, 100, NULL),
('20200505 01:00', 1, 100, NULL);


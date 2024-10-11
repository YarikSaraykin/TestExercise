
CREATE TABLE med.Groups 
(
	gr_id INT PRIMARY KEY,
    gr_name NVARCHAR(50),
    gr_temp NVARCHAR(50)
);

CREATE TABLE med.Analysis
(
	an_id INT PRIMARY KEY,
	an_name NVARCHAR(50),
	an_cost DECIMAL(10, 2),
	an_price DECIMAL(10, 2),
	an_group INT,
	FOREIGN KEY (an_group) REFERENCES med.Groups(gr_id)
);

CREATE TABLE med.Orders (
    ord_id INT PRIMARY KEY IDENTITY,  
    ord_datetime DATETIME,                 
    ord_an INT,                            
    FOREIGN KEY (ord_an) REFERENCES med.Analysis(an_id)
);






CREATE TABLE dbo.[CustomerCustomerDemo] (
	[CustomerID] NCHAR(5) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	[CustomerTypeID] NCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	CONSTRAINT [PK_CustomerCustomerDemo] PRIMARY KEY ([CustomerID], [CustomerTypeID]), 
	CONSTRAINT [FK_CustomerCustomerDemo] FOREIGN KEY([CustomerTypeID]) REFERENCES dbo.[CustomerDemographics] ([CustomerTypeID]), 
	CONSTRAINT [FK_CustomerCustomerDemo_Customers] FOREIGN KEY([CustomerID]) REFERENCES dbo.[Customers] ([CustomerID])
)


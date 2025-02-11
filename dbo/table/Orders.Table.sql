
CREATE TABLE dbo.[Orders] (
	[OrderID] INTEGER NOT NULL IDENTITY(1,1), 
	[CustomerID] NCHAR(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[EmployeeID] INTEGER NULL, 
	[OrderDate] DATETIME NULL, 
	[RequiredDate] DATETIME NULL, 
	[ShippedDate] DATETIME NULL, 
	[ShipVia] INTEGER NULL, 
	[Freight] MONEY NULL DEFAULT ((0)), 
	[ShipName] NVARCHAR(40) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[ShipAddress] NVARCHAR(60) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[ShipCity] NVARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[ShipRegion] NVARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[ShipPostalCode] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[ShipCountry] NVARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	CONSTRAINT [PK_Orders] PRIMARY KEY ([OrderID]), 
	CONSTRAINT [FK_Orders_Customers] FOREIGN KEY([CustomerID]) REFERENCES dbo.[Customers] ([CustomerID]), 
	CONSTRAINT [FK_Orders_Employees] FOREIGN KEY([EmployeeID]) REFERENCES dbo.[Employees] ([EmployeeID]), 
	CONSTRAINT [FK_Orders_Shippers] FOREIGN KEY([ShipVia]) REFERENCES dbo.[Shippers] ([ShipperID])
)


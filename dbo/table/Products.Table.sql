
CREATE TABLE dbo.[Products] (
	[ProductID] INTEGER NOT NULL IDENTITY(1,1), 
	[ProductName] NVARCHAR(40) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	[SupplierID] INTEGER NULL, 
	[CateryID] INTEGER NULL, 
	[QuantityPerUnit] NVARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[UnitPrice] MONEY NULL DEFAULT ((0)), 
	[UnitsInStock] SMALLINT NULL DEFAULT ((0)), 
	[UnitsOnOrder] SMALLINT NULL DEFAULT ((0)), 
	[ReorderLevel] SMALLINT NULL DEFAULT ((0)), 
	[Discontinued] BIT NOT NULL DEFAULT ((0)), 
	CONSTRAINT [PK_Products] PRIMARY KEY ([ProductID]), 
	CONSTRAINT [FK_Products_Cateries] FOREIGN KEY([CateryID]) REFERENCES dbo.[Cateries] ([CateryID]), 
	CONSTRAINT [FK_Products_Suppliers] FOREIGN KEY([SupplierID]) REFERENCES dbo.[Suppliers] ([SupplierID])
)


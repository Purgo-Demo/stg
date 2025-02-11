
CREATE TABLE dbo.[Order Details] (
	[OrderID] INTEGER NOT NULL, 
	[ProductID] INTEGER NOT NULL, 
	[UnitPrice] MONEY NOT NULL DEFAULT ((0)), 
	[Quantity] SMALLINT NOT NULL DEFAULT ((1)), 
	[Discount] REAL NOT NULL DEFAULT ((0)), 
	CONSTRAINT [PK_Order_Details] PRIMARY KEY ([OrderID], [ProductID]), 
	CONSTRAINT [FK_Order_Details_Orders] FOREIGN KEY([OrderID]) REFERENCES dbo.[Orders] ([OrderID]), 
	CONSTRAINT [FK_Order_Details_Products] FOREIGN KEY([ProductID]) REFERENCES dbo.[Products] ([ProductID])
)


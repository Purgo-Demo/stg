
CREATE TABLE dbo.[EmployeeTerritories] (
	[EmployeeID] INTEGER NOT NULL, 
	[TerritoryID] NVARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	CONSTRAINT [PK_EmployeeTerritories] PRIMARY KEY ([EmployeeID], [TerritoryID]), 
	CONSTRAINT [FK_EmployeeTerritories_Employees] FOREIGN KEY([EmployeeID]) REFERENCES dbo.[Employees] ([EmployeeID]), 
	CONSTRAINT [FK_EmployeeTerritories_Territories] FOREIGN KEY([TerritoryID]) REFERENCES dbo.[Territories] ([TerritoryID])
)


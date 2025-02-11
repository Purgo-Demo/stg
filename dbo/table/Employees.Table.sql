
CREATE TABLE dbo.[Employees] (
	[EmployeeID] INTEGER NOT NULL IDENTITY(1,1), 
	[LastName] NVARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	[FirstName] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	[Title] NVARCHAR(30) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[TitleOfCourtesy] NVARCHAR(25) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[BirthDate] DATETIME NULL, 
	[HireDate] DATETIME NULL, 
	[Address] NVARCHAR(60) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[City] NVARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[Region] NVARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[PostalCode] NVARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[Country] NVARCHAR(15) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[HomePhone] NVARCHAR(24) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[Extension] NVARCHAR(4) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[Photo] IMAGE NULL, 
	[Notes] NTEXT(1073741823) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[ReportsTo] INTEGER NULL, 
	[PhotoPath] NVARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	CONSTRAINT [PK_Employees] PRIMARY KEY ([EmployeeID]), 
	CONSTRAINT [FK_Employees_Employees] FOREIGN KEY([ReportsTo]) REFERENCES dbo.[Employees] ([EmployeeID])
)


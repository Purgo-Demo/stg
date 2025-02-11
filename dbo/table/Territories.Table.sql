
CREATE TABLE dbo.[Territories] (
	[TerritoryID] NVARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	[TerritoryDescription] NCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	[RegionID] INTEGER NOT NULL, 
	CONSTRAINT [PK_Territories] PRIMARY KEY ([TerritoryID]), 
	CONSTRAINT [FK_Territories_Region] FOREIGN KEY([RegionID]) REFERENCES dbo.[Region] ([RegionID])
)


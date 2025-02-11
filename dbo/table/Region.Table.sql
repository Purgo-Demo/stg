
CREATE TABLE dbo.[Region] (
	[RegionID] INTEGER NOT NULL, 
	[RegionDescription] NCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	CONSTRAINT [PK_Region] PRIMARY KEY ([RegionID])
)


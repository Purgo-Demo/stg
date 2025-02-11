
CREATE TABLE dbo.[CustomerDemographics] (
	[CustomerTypeID] NCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL, 
	[CustomerDesc] NTEXT(1073741823) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	CONSTRAINT [PK_CustomerDemographics] PRIMARY KEY ([CustomerTypeID])
)


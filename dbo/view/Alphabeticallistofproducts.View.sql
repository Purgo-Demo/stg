create view "Alphabetical list of products" AS
SELECT Products.*, Cateries.CateryName
FROM Cateries INNER JOIN Products ON Cateries.CateryID = Products.CateryID
WHERE (((Products.Discontinued)=0))


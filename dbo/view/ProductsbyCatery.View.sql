create view "Products by Catery" AS
SELECT Cateries.CateryName, Products.ProductName, Products.QuantityPerUnit, Products.UnitsInStock, Products.Discontinued
FROM Cateries INNER JOIN Products ON Cateries.CateryID = Products.CateryID
WHERE Products.Discontinued <> 1
--ORDER BY Cateries.CateryName, Products.ProductName


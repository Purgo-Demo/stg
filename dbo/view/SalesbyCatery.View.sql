create view "Sales by Catery" AS
SELECT Cateries.CateryID, Cateries.CateryName, Products.ProductName, 
	Sum("Order Details Extended".ExtendedPrice) AS ProductSales
FROM 	Cateries INNER JOIN 
		(Products INNER JOIN 
			(Orders INNER JOIN "Order Details Extended" ON Orders.OrderID = "Order Details Extended".OrderID) 
		ON Products.ProductID = "Order Details Extended".ProductID) 
	ON Cateries.CateryID = Products.CateryID
WHERE Orders.OrderDate BETWEEN '19970101' And '19971231'
GROUP BY Cateries.CateryID, Cateries.CateryName, Products.ProductName
--ORDER BY Products.ProductName
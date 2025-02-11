create view "Catery Sales for 1997" AS
SELECT "Product Sales for 1997".CateryName, Sum("Product Sales for 1997".ProductSales) AS CaterySales
FROM "Product Sales for 1997"
GROUP BY "Product Sales for 1997".CateryName


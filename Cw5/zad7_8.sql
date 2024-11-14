SELECT OrderDate, COUNT(*) AS OrdersCount
FROM AdventureWorksDW2019.dbo.FactInternetSales
GROUP BY OrderDate
HAVING COUNT(*) < 100
ORDER BY OrdersCount DESC;


SELECT OrderDate, ProductKey, UnitPrice
FROM (
    SELECT OrderDate, ProductKey, UnitPrice,
           ROW_NUMBER() OVER(PARTITION BY OrderDate ORDER BY UnitPrice DESC) AS Rank
    FROM AdventureWorksDW2019.dbo.FactInternetSales
) AS RankedProducts
WHERE Rank <= 3;


USE AdventureWorksDW2019;
GO

CREATE PROCEDURE usp_GetCurrencyRatesForGBPAndEUR
    @YearsAgo INT
AS
BEGIN
    SET NOCOUNT ON;

    -- Ustal datę sprzed 'YearsAgo' lat od dziś
    DECLARE @DateThreshold DATE = DATEADD(YEAR, -@YearsAgo, GETDATE());

    -- Pobranie kursów walut GBP i EUR sprzed określonej liczby lat
    SELECT 
        fcr.CurrencyKey,
        fcr.Date,
        fcr.AverageRate,
        fcr.EndOfDayRate,
        dc.CurrencyAlternateKey
    FROM 
        dbo.FactCurrencyRate AS fcr
    INNER JOIN 
        dbo.DimCurrency AS dc ON fcr.CurrencyKey = dc.CurrencyKey
    WHERE 
        dc.CurrencyAlternateKey IN ('GBP', 'EUR') -- Filtr na GBP i EUR
        AND fcr.Date <= @DateThreshold;           -- Filtr na datę sprzed 'YearsAgo' lat
END;
GO


EXEC usp_GetCurrencyRatesForGBPAndEUR @YearsAgo = 5;

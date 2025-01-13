# Parametry
$IndexNumber = "403438"
$Timestamp = (Get-Date -Format "MMddyyyyHHmmss")
$DownloadUrl = "http://home.agh.edu.pl/~wsarlej/dyd/bdp2/materialy/cw10/InternetSales_new.zip"
$DownloadPath = "./InternetSales_new.zip"
$ExtractPath = "./Extracted"
$ProcessedPath = "./PROCESSED"
$Password = "bdp2agh"
$LogFileName = "$ProcessedPath/script_log_$Timestamp.log"
$TableName = "CUSTOMERS_$IndexNumber"
$CsvOutput = "$ProcessedPath/Export_$Timestamp.csv"
$SqlServer = "ASUSSG\MSSQLSERVER01"
$DatabaseName = "AdventureWorksDW2019"

$SevenZipPath = (Get-Command 7z.exe).Source



# Tworzenie katalogów, jeśli nie istnieją
New-Item -ItemType Directory -Force -Path $ExtractPath, $ProcessedPath

# Funkcja do logowania
function Log-Step {
    param (
        [string]$Message,
        [string]$Status
    )
    $LogEntry = "[$(Get-Date -Format "yyyy-MM-dd HH:mm:ss")] $Message - $Status"
    $LogEntry | Out-File -Append -FilePath $LogFileName
    Write-Output $LogEntry
}


# Krok a: Pobieranie pliku
Log-Step -Message "Downloading file from $DownloadUrl" -Status "Started"
try {
    Invoke-WebRequest -Uri $DownloadUrl -OutFile $DownloadPath
    Log-Step -Message "Downloading file from $DownloadUrl" -Status "Successful"
} catch {
    Log-Step -Message "Downloading file from $DownloadUrl" -Status "Failed"
    throw $_
}

# Krok b: Rozpakowanie pliku zip
Log-Step -Message "Extracting file to $ExtractPath" -Status "Started"
try {
    & $SevenZipPath x $DownloadPath -p"$Password" -o"$ExtractPath" -y
    Log-Step -Message "Extracting file to $ExtractPath" -Status "Successful"
} catch {
    Log-Step -Message "Extracting file to $ExtractPath failed: $_" -Status "Failed"
    throw "Extraction failed. Error: $_"
}

# Walidacja rozpakowanego katalogu
$ExtractedFiles = Get-ChildItem -Path $ExtractPath -Recurse
if ($ExtractedFiles.Count -eq 0) {
    Log-Step -Message "No files found in $ExtractPath after extraction" -Status "Failed"
    throw "Extraction completed but no files were found in $ExtractPath."
} else {
    Log-Step -Message "$($ExtractedFiles.Count) files extracted to $ExtractPath" -Status "Successful"
}

# Usuwanie tymczasowego pliku ZIP
if (Test-Path $DownloadPath) {
    Remove-Item $DownloadPath -Force
    Log-Step -Message "Temporary file $DownloadPath removed" -Status "Successful"
}

# Krok c: Walidacja i przetwarzanie pliku
$InputFile = "$ExtractPath/InternetSales_new.txt"
$ValidFile = "$ProcessedPath/InternetSales_new_valid_$Timestamp.txt"
$BadFile = "$ProcessedPath/InternetSales_new.bad_$Timestamp.txt"

Log-Step -Message "Validating and processing file $InputFile" -Status "Started"
try {
    $Header = Get-Content $InputFile -TotalCount 1
    $ExpectedColumns = $Header -split '\|'
    
    # Kolekcja do przechowywania unikalnych wierszy
    $SeenLines = @()
    # Funkcja do sprawdzania wartości liczbowych
function Is-Numeric {
    param ([string]$Value)
    return $Value -match "^\d+(\.\d+)?$"
}

    Get-Content $InputFile |
        Where-Object { $_ -and $_ -ne $Header } |
        ForEach-Object {
            $Columns = $_ -split '\|'
            
            # Sprawdzamy, czy $Columns[6] nie jest pusty
            if (-not [string]::IsNullOrEmpty($Columns[6])) {
                # Jeśli już widzieliśmy ten wiersz, pomijamy go
                if ($SeenLines -contains $_) {
                    return  # Kontynuuje następną iterację pętli
                }

                # Zapisujemy do $BadFile, jeśli wiersz nie jest pusty
                $_ | Out-File -Append -FilePath $BadFile
                $SeenLines += $_  # Dodajemy wiersz do kolekcji unikalnych wierszy
                return
            }

            # Jeśli wiersz spełnia inne warunki i jest unikalny
            if ($Columns.Count -eq $ExpectedColumns.Count -and
                [int]$Columns[4] -le 100 -and
                (Is-Numeric $Columns[0]) -and
                (Is-Numeric $Columns[3]) -and
                (Is-Numeric $Columns[4]) -and
                (Is-Numeric $Columns[5].Replace(",", "."))) {

                $CustomerName = $Columns[2] -replace '"', ''
                if ($CustomerName -match '^(?<LastName>[^,]+),(?<FirstName>.+)$') {
                    $Columns[2] = $Matches['FirstName'] + '|' + $Matches['LastName']
                    $Columns[5] = $Columns[5] -replace ',', '.'
                    $NewLine = $Columns -join '|'

                    # Jeśli wiersz nie był jeszcze widziany, dodajemy do wyników
                    if ($SeenLines -notcontains $NewLine) {
                        $SeenLines += $NewLine
                        $NewLine
                    }
                }
            } else {
                # Zapisujemy do $BadFile w przypadku innych błędów
                $_ | Out-File -Append -FilePath $BadFile
            }
        } | Set-Content -Path $ValidFile

    Log-Step -Message "Validating and processing file $InputFile" -Status "Successful"
} catch {
    Log-Step -Message "Validating and processing file $InputFile" -Status "Failed"
    throw $_
}

# Tworzenie zapytania SQL do tworzenia tabeli
$createTableQuery = @"
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '$TableName')
BEGIN
    CREATE TABLE $TableName (
        ProductKey INT,
        CurrencyAlternateKey VARCHAR(10),
        FIRST_NAME VARCHAR(100),
        LAST_NAME VARCHAR(100),
        OrderDateKey INT,
        OrderQuantity INT,
        UnitPrice DECIMAL(10,2),
        SecretCode VARCHAR(10)
    )
END
"@

# Tworzenie tabeli w SQL Server
Log-Step -Message "Creating table $TableName in SQL Server" -Status "Started"
try {
    # Connection string z użyciem Windows Authentication
    $connectionString = "Server=$SqlServer;Database=$DatabaseName;Integrated Security=True;"

    # Tworzenie połączenia
    $connection = New-Object System.Data.SqlClient.SqlConnection
    $connection.ConnectionString = $connectionString
    $connection.Open()

    # Tworzenie komendy SQL
    $command = $connection.CreateCommand()
    $command.CommandText = $createTableQuery

    # Wykonanie zapytania
    $command.ExecuteNonQuery()
    Log-Step -Message "Creating table $TableName in SQL Server" -Status "Successful"

    # Zamknięcie połączenia
    $connection.Close()
} catch {
    Log-Step -Message "Creating table $TableName in SQL Server" -Status "Failed"
    Write-Error $_
    throw $_
}

$BasePath = $PSScriptRoot
$ValidFile = $ValidFile -replace "/", "\" -replace "^\.\\", ""
$FullPath = Join-Path -Path $BasePath -ChildPath $ValidFile



# Krok e: Załadowanie danych do SQL Server
Log-Step -Message "Loading data into SQL Server table $TableName" -Status "Started"
try {
    $bulkInsertQuery = @"
    BULK INSERT $TableName
    FROM '$FullPath'

    WITH (
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '\n',
        FIRSTROW = 2
    );
"@

    $command.CommandText = $bulkInsertQuery
    $connection.Open()
    $command.ExecuteNonQuery()
    Log-Step -Message "Loading data into SQL Server table $TableName" -Status "Successful"
    $connection.Close()
} catch {
    Log-Step -Message "Loading data into SQL Server table $TableName" -Status "Failed"
    Write-Error $_
    throw $_
}

# Krok f: Przeniesienie przetworzonego pliku
# Krok wykonany w trakcie przetwarzania pliku [krok c]

# Krok g: Aktualizacja kolumny SecretCode w SQL Server
Log-Step -Message "Updating SecretCode column in table $TableName" -Status "Started"
try {
    $updateQuery = "UPDATE $TableName SET SecretCode = SUBSTRING(CONVERT(VARCHAR(36), NEWID()), 1, 10);"
    $command.CommandText = $updateQuery
    $connection.Open()
    $command.ExecuteNonQuery()
    Log-Step -Message "Updating SecretCode column in table $TableName" -Status "Successful"
    bcp AdventureWorksDW2019.dbo.CUSTOMERS_403438 format nul -T -w -f Customers.fmt
    $connection.Close()
} catch {
    Log-Step -Message "Updating SecretCode column in table $TableName" -Status "Failed"
    Write-Error $_
    throw $_
}


#Usuwanie zbednych plikow
Remove-Item $ExtractPath -Recurse -Force

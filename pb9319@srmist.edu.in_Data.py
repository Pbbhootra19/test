import mysql.connector
from mysql.connector import Error

# MySQL connection details
host = 'localhost'
user = 'root'
password = 'admin'
database = 'policy'

try:
    # Connect to MySQL server
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    cursor = connection.cursor()

#     # I. ProductGroup with highest sale numbers
#     query1 = "SELECT COALESCE(GROUP_CONCAT(ProductGroup ORDER BY MembersCovered DESC SEPARATOR '-'), 'NA') AS Metric1 FROM (SELECT ProductGroup, SUM(MembersCovered) AS MembersCovered FROM product GROUP BY ProductGroup ORDER BY MembersCovered DESC LIMIT 1) AS subquery"
#     cursor.execute(query1)
#     metric1 = cursor.fetchone()[0]

#     # II. ProductGroup with highest number of claims in Year 2022
#     query2 = "SELECT COALESCE(GROUP_CONCAT(ProductGroup ORDER BY <ClaimCountColumn> DESC SEPARATOR '-'), 'NA') AS Metric2 FROM (SELECT p.ProductGroup, COUNT(c.ClaimId) AS <ClaimCountColumn> FROM product p LEFT JOIN claim c ON p.ProductId = c.ProductId WHERE YEAR(c.ClaimDate) = 2022 GROUP BY p.ProductGroup ORDER BY <ClaimCountColumn> DESC LIMIT 1) AS subquery"
#     cursor.execute(query2)
#     metric2 = cursor.fetchone()[0]

#     # III. Total revenue generated through Premium in Year 2022
#     query3 = "SELECT COALESCE(SUM(Premium), 0) AS Metric3 FROM policy WHERE YEAR(PurchaseDate) = 2022"
#     cursor.execute(query3)
#     metric3 = str(cursor.fetchone()[0])

#     # IV. Number of Product Groups where at least 1 claim is registered
#     query4 = "SELECT COUNT(DISTINCT ProductGroup) AS Metric4 FROM product WHERE ProductId IN (SELECT DISTINCT ProductId FROM claim)"
#     cursor.execute(query4)
#     metric4 = str(cursor.fetchone()[0])

#     # V. Number of Policies where at least 1 claim is approved in Month of Jan - 2023
#     query5 = "SELECT COUNT(DISTINCT PolicyId) AS Metric5 FROM claim WHERE YEAR(ClaimApprovedDate) = 2023 AND MONTH(ClaimApprovedDate) = 1 AND ClaimStatus = 'Approved'"
#     cursor.execute(query5)
#     metric5 = str(cursor.fetchone()[0])

#     # VI. Number of unique customers who registered claims as of today
#     query6 = "SELECT COUNT(DISTINCT CustomerId) AS Metric6 FROM claim"
#     cursor.execute(query6)
#     metric6 = str(cursor.fetchone()[0])

#     VII. Number of policies which have not registered a single claim
#     query7 = "SELECT COUNT(*) A FROM policy WHERE PolicyId NOT IN (SELECT DISTINCT PolicyId FROM claim)"
#     cursor.execute(query7)
#     metric7 = str(cursor.fetchone()[0])

    # VIII. Number of unique Products which have active policies, but no claims associated
    query8 = "SELECT COUNT(DISTINCT ProductId) AS Metric8 FROM product WHERE ProductId IN (SELECT DISTINCT ProductId FROM policy) AND ProductId NOT IN (SELECT DISTINCT ProductId FROM claim)"
    cursor.execute(query8)
    metric8 = str(cursor.fetchone()[0])

    # IX. Number of Unique policies where claim was registered after policy expiration
    query9 = "SELECT COUNT(DISTINCT c.PolicyId) AS Metric9 FROM policy p INNER JOIN claim c ON p.PolicyId = c.PolicyId WHERE c.ClaimDate > p.ExpiryDate"
    cursor.execute(query9)
    metric9 = str(cursor.fetchone()[0])

    # X. Number of policies where Total claimed amount is 100% or more than of Policy premium amount
    query10 = "SELECT COUNT(DISTINCT p.PolicyId) AS Metric10 FROM policy p INNER JOIN (SELECT PolicyId, SUM(ClaimAmount) AS TotalClaimAmount FROM claim GROUP BY PolicyId) c ON p.PolicyId = c.PolicyId WHERE c.TotalClaimAmount >= p.Premium"
    cursor.execute(query10)
    metric10 = str(cursor.fetchone()[0])

    # Concatenate the metrics into a single column with hyphen separator
    data_metrics = f"{metric8}-{metric9}-{metric10}"
    print("Data Metrics:", data_metrics)

except Error as e:
    print("Error connecting to MySQL:", e)

finally:
    # Close the connection and cursor
    if connection.is_connected():
        connection.close()
    cursor.close()

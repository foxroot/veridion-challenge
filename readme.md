Task, given a company data parquet file, we want to remove the duplicate companies

First steps:

The first step is to look at the data and see which fields can be used to base the de-duplication process on.
In the beginning I think that the uniqueness of a company, in the absence of something like VAT number, is given by the combination of two columns: company name and company country.
My reasoning is that there can be companies with the same name registered in different countries but there cannot be multiple companies with the same name in the same country. When creating a company, only a unique registration name is accepted within the registration country.

I next check an actual example of a company.
For example, we have 5 entries for the company "Club Tarneit". Disregarding the fields which are not populated in all entries (like street), some fields like company name, company country code, website domain and postcode are the same, whereas some are different like main business category (dance clubs / pubs / wedding venues) and main industry (pubs vs. restaurants). I look this company up and it looks like it's the same company, a club which also organises functions such as weddings.
The city is also different, for four entries we've got City Of Wyndham, while one entry has Melbourne (City Of Wyndham is within Melbourne).

Next I want to check if there are nulls in company name, company country code, and website domain.
There are 829 nulls in company name.
There are 2031 nulls in country code
There are 1553 nulls in website domain


Before going forwards, I convert all company names into lowercase and remove the punctuation from the company to avoid making unique records caused by a different case & punctuation e.g. Apple Inc vs apple inc.
Then, I improve the performance by partitioning by country code.

Next, I count all rows: the row count of all rows is 33446
I come up with 3 cases and some sub-cases:
1.  Non-null company and non-null country code. In this case we can do the de-duplication by country code + company
The row count: 30821
These are the two fields which make a company unique
coalesce(company_name,company_legal_names,company_commercial_names) leads to the same number of rows so I don't include this in the code
and coalesce(main_country_code,main_country) leads to the same number of rows so I don't include this in the code

There can be the same postcodes in different countries so company name + postcode would not be unique.
The field locations from the company data looks like a concatenation of the address fields, some of which are null, so locations contains empty spaces, which could create duplicates.
The row count of the unique company name + country code combinations: 10662
The row count of the duplicate company name + country code combinations: 20159
The row count for the remaining data, not accounted so far: 2625
2. Null company name
The row count: 829. There are two sub-cases:
- the website is not null - we use the website domain for the deduplication as long as this is not part of the domains from case 1. Row count: 55
- the website is null - we exclude these fields from the main data, and add them to dupes data. Row count: 213

The country code can be either null or not.
Here, I assume that the relationship between the company and country is 1:1 but the relationship between the company and the website is one:many so this means multiple websites can actually be associated with the same company but we don't know the company here so this can also create duplicates.
3. Non null company name
The row count: 1796
 - we do the de-duplication on company only if the company is not already in the main company data. Row count: 563 - these will be added later to the company data. The duplicate records will be added to the dupes dataframe - row count - 1233
N.B. I assume that a company can have many websites with different domains so company + website domain would not be unique

In the end:
- row count unique company data: 11280
- row count duplicate company data: 22166

Caveats:
This solution does not take into consideration cases like e.g. there are two similar company names e.g. Avalon Incorporated and Avalon Inc. The solution classified these as two distinct companies but they might not be.

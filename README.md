# HMDA Data Analytics Project


### Data Details

Below is the data dictionary for the data: 

#### HMDA Combined MLAR - 2023

* **activity_year** (int) - The year of the dataset
* **legal_entity_identifier** (str) - Unique 20-character ID for a legal financial entity which tracks the lender for each mortgage application
* **loan_type** (int) - The type of loan
    * 1 - Conventional (not insured or guaranteed by FHA, VA, RHS, or FSA)
    * 2 - Federal Housing Administration insured (FHA)
    * 3 - Veterans Affairs guaranteed (VA)
    * 4 - USDA Rural Housing Service or the Farm Service Agency guaranteed (RHS or FSA)
* **loan_purpose** (int) - The purpose of the loan
    * 1 - Home Purchase
    * 2 - Home improvement
    * 31 - Refinancing
    * 32 - Cash-out refinancing
    * 4 - Other purpose
    * 5 - Not applicable
* **preapproval** (int) - Whether preapproval was requested or not
    * 1 - Preapproval requested
    * 2 - Preapproval not requested
* **construction_method** (int) - The type of construction used for the property
    * 1 - Site-built
    * 2 - Manufactured home
* **occupancy_type** (int) - The type of occupancy of the borrower for the loan requested on the property
    * 1 - Principal residence
    * 2 - Second residence
    * 3 - Investment property
* **loan_amount** (int) - The reported value of the requested loan amount.  This is reported by the midpoint for the $10000 interval into which the reported value falls.  Example: If a disclosed value was $117,834 it would reported as $115,000 which is the midpoint between $110,000 and $120,000
* **action_taken** (int) - The action taken by the lender for the loan
   * 1 - Loan Originated - This means the mortgage application was approved, the borrower accepted the offer, and the loan funds were officially disbursed.  This also means all underwriting steps were completed, the borrower signed the final documents, and the lender offically released the money.
   * 2 - Application approved but not accepted
   * 3 - Application denied
   * 4 - Application withdrawn by applicant
   * 5 - File closed for incompleteness
   * 6 - Purchased loan (not originated by the institution)
   * 7 - Preapproval request denied
   * 8 - Preapproval request approved but not accepted
* **property_state** (str) - The state where the property is located.  For this dataset all rows will be WI.
* **property_county** (int) - The county where the property is located which is designated by the county FIPS code.
* **census_tract** (int) - This is a code which represents a permanent geographic area created by the US Census Bureau to collect and analyze data about neighborhood-level populations.  Each area is designed to have about 1,200 to 8,000 people and the boundaries usually follow visible features or governmental boundaries.  This code is designated by the first 2 digits being the State FIPS code, the next 3 digits being the County FIPS code, and the last 6 digits being the Tract code (decimal structure).
    * NOTE: There are 8 rows which are null within our population
* **borrower_X_ethnicity** (int) - The ethnicity of the borrower.  There are five columns with this representation since there can be up to five borrowers on a loan. NOTE: There are 9 nulls in the `borrower_one_ethnicity` column. Additionally, there are zero instances of a borrower being in `borrower_five_ethnicity`
    * 1 - Hispanic or Latino
    * 2 - Not Hispanic or Latino
    * 3 - Information not provided by applicant
    * 4 - Not applicable - Selected if the applicant or borrower did not select any ethnicities and only provided ethnicities in the Ethnicity of Applicant or Borrower
    * 11 - Mexican
    * 12 - Puerto Rican
    * 13 - Cuban
    * 14 - Other Hispanci or Latino
* **co_borrower_X_ethnicity** (int) - The ethnicity of the co-applicant or co-borrower.  There are five columns with this representation since there can be up to five borrowers on a loan.  NOTE: There are 2 nulls in the `co_borrower_one_ethnicity` column.  Additionally, there are zero instances of a co-borrower being in `co_borrower_five_ethnicity`
    * 1 - Hispanic or Latino
    * 2 - Not Hispanic or Latino
    * 3 - Information not provided by applicant
    * 4 - Not applicable - Selected if the applicant or borrower did not select any ethnicities and only provided ethnicities in the Ethnicity of Applicant or Borrower
    * 5 - No co-applicant.  If the co-applicant or co-borrower did not select any ethnicity, but only provided ethnicity in the Ethnicity of Co-applicant or Co-borrower.  They either leave this data field blank or they enter code 14.
    * 11 - Mexican
    * 12 - Puerto Rican
    * 13 - Cuban
    * 14 - Other Hispanci or Latino
* **borrower_ethnicity_based_on_obs** (int) - An indicator of whether the ethnicity of the applicant or borrower was collected on the basis of visual observation or Surname.
    * 1 - Collected on the basis of visual observation or surname.
    * 2 - Not collected on the basis of visual observation or surname.
    * 3 - Not applicable
* **co_borrower_ethnicity_based_on_obs** (int) - An indicator of whether the ethnicity of the co-applicant or co-borrower was collected on the basis of visual observation or Surname.
    * 1 - Collected on the basis of visual observation or surname.
    * 2 - Not collected on the basis of visual observation or surname.
    * 3 - Not applicable
    * 4 - No co-applicant
* **borrower_X_race** (int) - The race of the applicant or borrower.
    * 1 - American Indian or Alaska Native
    * 2 - Asian
    * 3 - Black or African American
    * 4 - Native Hawaiian or Other Pacific Islander
    * 5 - White 
    * 6 - Information not provided by applicant in the application
    * 7 - Not applicable.  If the applicant or borrower did not select any race and only provided race in the Race of applicant or borrower free form text field.
    * 21 - Asian Indian
    * 22 - Chinese
    * 23 - Filipino
    * 24 - Japanese
    * 25 - Korean
    * 26 - Vietnamese
    * 27 - Other Asian


### Task List

1. Join FIPS data to the original HMDA dataframe
2. Join LEI data to the original HMDA dataframe
3. After analysis, complete data modeling for the HMDA dataframe
4. Create a processing script which can handle doing all of this cleaning and preprocessing
5. Identify which census tract locations are in each alderman district
6. Process the census tract data to get more information

### Possible Modeling Questions
* Create a model which can predict whether a loan will be approved or denied

### Feature Engineering
* Get additional data on the Census Tract locations
* Create a feature based on `borrower_X_ethnicity` which indicates the number of borrowers present on the loan.

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
* **co_borrower_X_race** (int) - The race of the co-applicant or co-borrower
    * 1 - American Indian or Alaska Native
    * 2 - Asian
    * 3 - Black or African American
    * 4 - Native Hawaiian or Other Pacific Islander
    * 5 - White 
    * 6 - Information not provided by applicant in the application
    * 7 - Not applicable.  If the applicant or borrower did not select any race and only provided race in the Race of applicant or borrower free form text field.
    * 8 - No co-applicant
    * 21 - Asian Indian
    * 22 - Chinese
    * 23 - Filipino
    * 24 - Japanese
    * 25 - Korean
    * 26 - Vietnamese
    * 27 - Other Asian
* **borrower_race_based_on_obs** (int) - Race of applicant or borrower collected on the basis of visual observation or Surname
    * 1 - Collected on the basis of visual observation or surname
    * 2 - Not collected on the basis of visual observation or surname
    * 3 - Not applicable
* **co_borrower_race_based_on_obs** (int) - Race of co-applicant or co-borrower collected on the basis of visual observation or Surname
    * 1 - Collected on the bsis of visual observation or surname.
    * 2 - Not collected on the basis of visual observation or surname.
    * 3 - Not applicable. 
    * 4 - No co-applicant.
* **borrower_gender** (int) - Gender of applicant or borrower
    * 1 - Male
    * 2 - Female
    * 3 - Information not provided by applicant on application.
    * 4 - Not applicable
    * 6 - Applicant selected both male and female
* **co_borrower_gender** (int) - Gender of co-applicant or co-borrower
    * 1 - Male
    * 2 - Female
    * 3 - Information not provided by applicant on application.
    * 4 - Not applicable
    * 5 - No co-applicant
    * 6 - Applicant selected both male and female
* **borrower_gender_based_on_obs** (int) - Gender of applicant or borrower collected on the basis of visual observation or Surname.
    * 1 - Collected on the basis of visual observation or surname
    * 2 - Not collected on the basis of visual observation or surname
    * 3 - Not applicable
* **co_borrower_gender_based_on_obs** (int) - Gender of co-applicant or co-borrower collected on the basis of visual observation or Surname.
    * 1 - Collected on the basis of visual observation or surname
    * 2 - Not collected on the basis of visual observation or surname
    * 3 - Not applicable
    * 4 - No co-applicant
* **borrower_age** (str -> int) - Age of the applicant or borrower.  This is based on different bins depending on the age.  If an age is not reported, it is recorded as 8888.
    * Possible values: <25, 25-34, 35-44, 45-54, 55,-64, 65-74, >74, 8888
* **is_borrower_62_or_older** (str -> int) - Indicates whether the age of the applicant is greater than or equal to 62.  NA is entered if the age of applicant or borrower is 8888. There are currently 3,000+ null values in this column.  It is represented by either Yes or No and will be converted into an integer to be a categorical representation.
* **co_borrower_age** (str -> int) - Age of the co-applicant or co-borrower.  This is based on different bins depending on the age.  If an age is not reported, it is indicated by 8888.  If there is not a co-applicant or co-borrower, it is indicated by 9999.
    * Possible values: <25, 25-34, 35-44, 45-54, 55,-64, 65-74, >74, 8888, 9999
* **is_co_borrower_62_or_older** (str -> int) -   Indicates whether the age of the co-applicant or co-borrower is greater than or equal to 62.  NA is entered if the age of applicant or borrower is 8888 or 9999 (no co-borrower or co-applicant).  There are currently 19044 null values in this column.  It is represented by either Yes or No and will be converted into an integer to be a categorical representation.
* **income** (int) - The income of the borrower represented in thousands of dollars.  NOTE: There are currently 3355
null values in this column.
* **type_of_purchaser** (int) - Indicates the type of purchaser.  In the context of this data, it is who bought the mortgage loan after it was originated i.e. if it was sold to another financial institution.
    * 0 - Not applicable
    * 1 - Fannie Mae
    * 2 - Ginnie Mae
    * 3 - Freddie Mac
    * 4 - Farmer Mac
    * 5 - Private securitizer
    * 6 - Commerical bank savings bank or savings association
    * 8 - Affiliate institution
    * 9 - Other type of purchaser
    * 71 - Credit union mortgage company or finance company
    * 72 - Life insurance company
* **rate_spread** (float) - Rate spread is the difference between the Annual Percentage Rate (APR) on the loan and the Average Prime Offer Rate (APOR) for a comparable loan at the time the loan was made.  It measures how much higher loan's interest cost is compared to what a "best qualified borrower" would get at the same time. For HMDA rules, a loan is considered higher-priced if APR > (APOR + 1.5 percentage points) - this is for the standard mortgage.  NOTE: There are currently 13525 null values in the dataset.
* **hoepa_status** (int) - HOEPA is the Home Ownership and Equity Protection Act of 1994.  This column indicates whether a loan is classified as "high-cost mortgage" under federal consumer protection laws.  This indication is yes if the APR exceeds the APOR by a big threshold or it charges points and fees above a certain percentage of the total loan amount. 
    1 - High-cost mortgage
    2 - Not a high-cost mortgage
    3 - Not applicable
* **lien_status** (int) - What position the loan holds against the property in terms of repayment priority if the borrower defaults.  
    1 - Secured by a first lien - primary loan secured by the property
    2 - Secured by a subordinate lien - secondary loan behind the first one (home equity) - paid only after the first lien is satisfied
* **borrower_credit_score_model** (int) - The name and version of the credit scoring model used for the applicant or borrower
    1 - Equifax Beacon 5.0
    2 - Experian Fair Isaac
    3 - FICO Risk Score Classic 04
    4 - FICO Risk Score Classic 98
    5 - VantageScore 2.0
    6 - VantageScore 3.0
    7 - More than one credit scoring model
    8 - Other credit scoring model
    9 - Not applicable
* **co_borrower_credit_score_model** (int) - The name and version of the credit scoring model used for the co-applicant or co-borrower
    1 - Equifax Beacon 5.0
    2 - Experian Fair Isaac
    3 - FICO Risk Score Classic 04
    4 - FICO Risk Score Classic 98
    5 - VantageScore 2.0
    6 - VantageScore 3.0
    7 - More than one credit scoring model
    8 - Other credit scoring model
    9 - Not applicable
    10 - No co-applicant
* **reason_for_denial_X** (int) - The reason the loan was denied.  There are four columns which represent this information. Columns two-four will have null values.
    1 - Debt-to-income ratio
    2 - Employment history
    3 - Credit history
    4 - Collateral
    5 - Insufficient cash (downpayment closing costs) 
    6 - Unverifiable information
    7 - Credit application incomplete
    8 - Mortgage insurance denied
    9 - Other
    10 - Not applicable
* **total_loan_costs** (float) - Total loan costs. NOTE: 17086 null values.
* **total_points_and_fees** (float) - Total points and fees for the loan. NOTE: 28246 null values.
* **origination_charges** (float) - Fees the lender charges the borrower for making the loan. NOTE: 17336 null values.
* **discount_points** (float) - Points discounted on the loan. NOTE: 23360 null values.
* **lender_credits** (float) - The amount of money that the lender gives to the borrower at closing to offset closing costs. NOTE: 23666 null values.
* **interest_rate** (float) - The interest rate on the loan. NOTE: 9162 null values.
* **prepayment_penalty_term** (int) - How long after loan origination the borrower would have to pay a penalty if they pay off the loan early.  This penalty goes into affect if the borrower refinances, sells the home and pays off the mortgage, or pays off the loan balance early. This is measured in months. NOTE: 26001 null values.
* **debt_to_income_ratio** (str -> int) - This is the borrower's monthly debt obligations divided by their monthly gross income (before taxes).  In this dataset, it is on the back-end meaning it includes all monthly debts to include mortgage payment, credit card minimum payments, car loans, student loans, etc. NOTE: 7376 null values. This is measured in bins within the dataset: 
    * Possible values: <20%, 20-<30%, 30%-<36%, 50%-<60%, >60%
    * Could also be the exact value if not exact
* **combined_loan_to_value_ratio** (float) - The total amount of all secured loans on a property, divided by the property's appraised value or purchase price (whichever is lower), expressed as a percentage. NOTE: 6804 null values.
* **loan_term_months** (int) - The number of months for the life of the loan. NOTE: 227 null values
* **intro_rate_period** (int) - The number of months during which the loan's initial interest rate is fixed before it may adjust. NOTE: 19211 null values.
* **balloon_payment** (int) - An indicator of whether the loan has a balloon payment. A balloon payment is a large lump-sump payment due at the end of a loan's term, after a period of smaller regualar payments.
    * 1 - Balloon Payment
    * 2 - No balloon payment
    * 1111 - Not known
* **interest_only_payments** (int) - An indicator of whether the borrower's required initial payments only cover the loan interest, and do not reduce the loan principle for a period of time.
    * 1 - interest-only payments
    * 2 - No interest-only payments
    * 1111 - Not-applicable
* **negative_amortization** (int) - Indicates whether the loan has negative amortization.  This is where the borrower's monthly payments are not enough to cover even the full interest due on the loan.  The unpaid interest is added to the loan principal, causing the total balance to grow over time instead of shrinking.
    * 1 - Negative amortization
    * 2 - No negative amortization
    * 1111 - Not applicable
* **other_non_amortizing_features** (int) - Indicates whether there are loan terms that delay or reduce the normal repayment of the principal.  Borrowers make reduced, deferred, or interest-only payments meaning the principal balance doesn't decrease normally. 
    * 1 - Other non-fully amortizing features on the loan
    * 2 - No other non fully amortizing features
    * 1111 - Not applicable/known
* **property_value** (int) - The reported value of the property.  Represented as the midpoint for the $10000 interval into which the reported value falls. NOTE: 4630 null values present.
* **manufactured_home_property_type** (int) - Whether the property is considered manufactured home and land.
    * 1 - Manufactured home and land
    * 2 - Manufactured home and not land
    * 3 - Not applicable
    * 1111 - Unknown
* **manufactured_home_land_property_interest** (int) - Which ownership or rental arrangement the borrower has regarding the land where a manufactured home sits.
    * 1 - Direct ownership
    * 2 - Indirect ownership
    * 3 - Paid leasehold
    * 4 - Unpaid leasehold
    * 5 - Not applicable
    * 1111 - Unknown
* **total_units** (str -> int) - The number of units present on the property as represented by bins
    * Possible values: 1, 2, 3, 4, 5-24, 25-49, 50-99, 100-149, >149
* **multifamily_affordable_units** (int) - The number of affordable multifamily units present on the property. Represented as a percentage. NOTE: 28024 null values.
* **submission_of_application** (int) - The method of application submission.
    * 1 - Submitted directly to the institution
    * 2 - Not submitted directly to the institution
    * 3 - Not applicable
    * 1111 - Unknown
* **is_init_payable_to_institution** (int) - Who is the first entity that the borrower owes money to when the loan closes - the lender who will collect the first payments.  This is the lender listed on the loan.
    * 1 - First payments are payable to the reporting institution
    * 2 - The loan was originated on behalf of another institution or immediately assigned/sold
    * 3 - Not applicable
    * 1111 - Unknown
* **auto_underwriting_system_X** (int) - There are five of these columns and they represent the type of underwriting system used.  There will be null values in the 2-5 columns.
    * 1 - Desktop Underwriter (Fannie Mae)
    * 2 - Loan Product Advisor (Freddie Mac) 
    * 3 - Technology Open to Approved Lenders Scorecard - FHA
    * 4 - Guaranteed Underwriting System - USDA
    * 5 - Other proprietary AUS (non-agency)
    * 6 - Not applicable
    * 1111 - Unknown
* **is_reverse_mortgage** (int) - Whether the reported loan is a reverse mortgage, where the borrower receives payments from the lender, rather than making payments to the lender.  Often used when seniors want to stay in their homes without making monthly mortgage payments. 
    * 1 - Reverse mortgage
    * 2 - NOt a reverse mortgage
    * 1111 - Uknown
* **is_open_end_line_of_credit** (int) - Whether the reported loan is an open-end credit product like a home equity line of credit rather than a closed-end loan like a traditional mortgage. 
    * 1 - Open-end line of credit
    * 2 - Not an open end line of credit
    * 1111 - Unknown
* **is_for_business_purpose** (int) - Whether the loan was used for a business or commerical purpose. 
    * 1 - Primarily used for a business or commercial purpose. 
    * 2 - Not primarily for a business or commercial purpose
    * 1111 - Uknown



### Task List

1. Join FIPS data to the original HMDA dataframe
2. Join LEI data to the original HMDA dataframe
3. After analysis, complete data modeling for the HMDA dataframe
4. Create a processing script which can handle doing all of this cleaning and preprocessing
5. Identify which census tract locations are in each alderman district
6. Process the census tract data to get more information
7. Process the 2022 dataset and compare our output to the one DYCU provided.

### Data Quality Questions
1. Check to make sure if there isn't a co-applicant that it is consistent among all of the columns where that is possible. 
2. Look into the rows where the rate spread is being identified as a negative number (it shouldn't) 
3. For columns where `exempt` is an option, we need to make this a null value.

### Notes about the target
1. They are considering an origination as action_taken == 1, loan_pupose == 1, occupancy_type == 1
2. 


### Possible Modeling Questions
* Create a model which can predict whether a loan will be approved or denied

### Other potential analysis
* Average the rate spread values across lenders and analyze it

### Feature Engineering
* Get additional data on the Census Tract locations
* Create a feature based on `borrower_X_ethnicity` which indicates the number of borrowers present on the loan.
* Create a feature based on whether the borrower is part of a minority group
* Create a feature based on whether there is a co-applicant or not
* Weight the feature based on race, ethnicity or gender being collected via visual observation
* Consider binning the income data based on histogram analysis
* Consider binning the rate spread and creating a higher or not value
* Consider modifying the income data so it is an actual representation in thousands
* Create a variable based on the number of denials listed on the application
* We will have to handle the debt_to_income_ratio column in a unique manner because it's a combination of binned and actual data
* Create a boolean feature based on whether the loan is an adjustable rate mortgage.  This will be when the introductory rate period is different than the term months on the loan.
* Create a feature which indicates whether more than one underwriting system was used or not.

### Future Questions
* Would you also like me to show you a full risk profile matrix (combining negative amortization, balloon, interest-only, prepayment penalties) — the way regulators or fair lending auditors look at high-risk lending?



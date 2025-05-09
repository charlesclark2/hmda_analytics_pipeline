# Public HMDA - LAR Data Fields


### [activity\_year](#activity_year)[​](#activity_year "Direct link to activity_year")


* **Data Type**: `int`


* **Description:** The calendar year the data submission covers



| activity\_year | Value |
| --- | --- |
| 2017 | 2017 |
| 2018 | 2018 |
| 2019 | 2019 |
| 2020 | 2020 |
| 2021 | 2021 |
| 2022 | 2022 |
| 2023 | 2023 |
| 2024 | 2024 |




### [lei](#lei)[​](#lei "Direct link to lei")



* **Data Type**: `str`
* **Description:** A financial institution’s Legal Entity Identifier
* **Values:**
	+ Varying values


### [derived\_msa-md](#derived_msa-md)[​](#derived_msa-md "Direct link to derived_msa-md")


* **Data Type**: `int`
* **Description:** The 5 digit derived MSA (metropolitan statistical area) or MD (metropolitan division) code. An MSA/MD is an area that has at least one urbanized area of 50,000 or more population.
* **Values:**
	+ Varying values


### [state\_code](#state_code)[​](#state_code "Direct link to state_code")


* **Data Type**: `str`
* **Description:** Two-letter state code
* **Values:**
	+ Varying values


### [county\_code](#county_code)[​](#county_code "Direct link to county_code")


* **Data Type**: `int`
* **Description:** State-county FIPS code
* **Values:**
	+ Varying values


### [census\_tract](#census_tract)[​](#census_tract "Direct link to census_tract")


* **Data Type**: `int`
* **Description:** 11 digit census tract number
* **Values:**
	+ Varying values


### [derived\_loan\_product\_type](#derived_loan_product_type)[​](#derived_loan_product_type "Direct link to derived_loan_product_type")


* **Data Type**: `str -> int`
* **Description:** Derived loan product type from Loan Type and Lien Status fields for easier querying of specific records
* **Values:**
	+ Conventional:First Lien
	+ FHA:First Lien
	+ VA:First Lien
	+ FSA/RHS:First Lien
	+ Conventional:Subordinate Lien
	+ FHA:Subordinate Lien
	+ VA:Subordinate Lien
	+ FSA/RHS:Subordinate Lien


### [derived\_dwelling\_category](#derived_dwelling_category)[​](#derived_dwelling_category "Direct link to derived_dwelling_category")


* **Data Type**: `str -> int`
* **Description:** Derived dwelling type from Construction Method and Total Units fields for easier querying of specific records
* **Values:**
	+ Single Family (1-4 Units):Site-Built
	+ Multifamily:Site-Built (5+ Units)
	+ Single Family (1-4 Units):Manufactured
	+ Multifamily:Manufactured (5+ Units)


### [conforming\_loan\_limit](#conforming_loan_limit)[​](#conforming_loan_limit "Direct link to conforming_loan_limit")


* **Data Type**: `str -> int`
* **Description:** Indicates whether the reported loan amount exceeds the GSE (government sponsored enterprise) conforming loan limit
* **Values:**
	+ C (Conforming)
	+ NC (Nonconforming)
	+ U (Undetermined)
	+ NA (Not Applicable)


### [derived\_ethnicity](#derived_ethnicity)[​](#derived_ethnicity "Direct link to derived_ethnicity")


* **Data Type**: `str -> int`
* **Description:** Single aggregated ethnicity categorization derived from applicant/borrower and co-applicant/co-borrower ethnicity fields
* **Values:**
	+ Hispanic or Latino
	+ Not Hispanic or Latino
	+ Joint
	+ Ethnicity Not Available
	+ Free Form Text Only


### [derived\_race](#derived_race)[​](#derived_race "Direct link to derived_race")


* **Data Type**: `str -> int`
* **Description:** Single aggregated race categorization derived from applicant/borrower and co-applicant/co-borrower race fields
* **Values:**
	+ American Indian or Alaska Native
	+ Asian
	+ Black or African American
	+ Native Hawaiian or Other Pacific Islander
	+ White
	+ 2 or more minority races
	+ Joint
	+ Free Form Text Only
	+ Race Not Available


### [derived\_sex](#derived_sex)[​](#derived_sex "Direct link to derived_sex")


* **Data Type**: `str -> int`
* **Description:** Single aggregated sex categorization derived from applicant/borrower and co-applicant/co-borrower sex fields
* **Values:**
	+ Male
	+ Female
	+ Joint
	+ Sex Not Available


### [action\_taken](#action_taken)[​](#action_taken "Direct link to action_taken")


* **Data Type**: `int`
* **Description:** The action taken on the covered loan or application
* **Values:**
	+ 1 - Loan originated
	+ 2 - Application approved but not accepted
	+ 3 - Application denied
	+ 4 - Application withdrawn by applicant
	+ 5 - File closed for incompleteness
	+ 6 - Purchased loan
	+ 7 - Preapproval request denied
	+ 8 - Preapproval request approved but not accepted


### [purchaser\_type](#purchaser_type)[​](#purchaser_type "Direct link to purchaser_type")


* **Data Type**: `int`
* **Description:** Type of entity purchasing a covered loan from the institution
* **Values:**
	+ 0 - Not applicable
	+ 1 - Fannie Mae
	+ 2 - Ginnie Mae
	+ 3 - Freddie Mac
	+ 4 - Farmer Mac
	+ 5 - Private securitizer
	+ 6 - Commercial bank, savings bank, or savings association
	+ 71 - Credit union, mortgage company, or finance company
	+ 72 - Life insurance company
	+ 8 - Affiliate institution
	+ 9 - Other type of purchaser


### [preapproval](#preapproval)[​](#preapproval "Direct link to preapproval")


* **Data Type**: `int`
* **Description:** Whether the covered loan or application involved a request for a preapproval of a home purchase loan under a preapproval program
* **Values:**
	+ 1 - Preapproval requested
	+ 2 - Preapproval not requested


### [loan\_type](#loan_type)[​](#loan_type "Direct link to loan_type")


* **Data Type**: `int`
* **Description:** The type of covered loan or application
* **Values:**
	+ 1 - Conventional (not insured or guaranteed by FHA, VA, RHS, or FSA)
	+ 2 - Federal Housing Administration insured (FHA)
	+ 3 - Veterans Affairs guaranteed (VA)
	+ 4 - USDA Rural Housing Service or Farm Service Agency guaranteed (RHS or FSA)


### [loan\_purpose](#loan_purpose)[​](#loan_purpose "Direct link to loan_purpose")


* **Data Type**: `int`
* **Description:** The purpose of covered loan or application
* **Values:**
	+ 1 - Home purchase
	+ 2 - Home improvement
	+ 31 - Refinancing
	+ 32 - Cash-out refinancing
	+ 4 - Other purpose
	+ 5 - Not applicable


### [lien\_status](#lien_status)[​](#lien_status "Direct link to lien_status")


* **Data Type**: `int`
* **Description:** Lien status of the property securing the covered loan, or in the case of an application, proposed to secure the covered loan
* **Values:**
	+ 1 - Secured by a first lien
	+ 2 - Secured by a subordinate lien


### [reverse\_mortgage](#reverse_mortgage)[​](#reverse_mortgage "Direct link to reverse_mortgage")


* **Data Type**: `int`
* **Description:** Whether the covered loan or application is for a reverse mortgage
* **Values:**
	+ 1 - Reverse mortgage
	+ 2 - Not a reverse mortgage
	+ 1111 - Exempt


### [open\_end\_line\_of\_credit](#open-end_line_of_credit)[​](#open-end_line_of_credit "Direct link to open-end_line_of_credit")


* **Data Type**: `int`
* **Description:** Whether the covered loan or application is for an open-end line of credit
* **Values:**
	+ 1 - Open-end line of credit
	+ 2 - Not an open-end line of credit
	+ 1111 - Exempt


### [business\_or\_commercial\_purpose](#business_or_commercial_purpose)[​](#business_or_commercial_purpose "Direct link to business_or_commercial_purpose")

* **Data Type**: `int`
* **Description:** Whether the covered loan or application is primarily for a business or commercial purpose
* **Values:**
	+ 1 - Primarily for a business or commercial purpose
	+ 2 - Not primarily for a business or commercial purpose
	+ 1111 - Exempt


### [loan\_amount](#loan_amount)[​](#loan_amount "Direct link to loan_amount")

* **Data Type**: `int`
* **Description:** The amount of the covered loan, or the amount applied for
* **Values:**
	+ Varying values


### [combined\_loan\_to\_value\_ratio](#loan_to_value_ratio)[​](#combined_loan_to_value_ratio "Direct link to combined_loan_to_value_ratio")

* **Data Type**: `float`
* **Description:** The ratio of the total amount of debt secured by the property to the value of the property relied on in making the credit decision
* **Values:**
	+ Varying values


### [interest\_rate](#interest_rate)[​](#interest_rate "Direct link to interest_rate")

* **Data Type**: `float`
* **Description:** The interest rate for the covered loan or application
* **Values:**
	+ Varying values


### [rate\_spread](#rate_spread)[​](#rate_spread "Direct link to rate_spread")

* **Data Type**: `float`
* **Description:** The difference between the covered loan’s annual percentage rate (APR) and the average prime offer rate (APOR) for a comparable transaction as of the date the interest rate is set
* **Values:**
	+ Varying values


### [hoepa\_status](#hoepa_status)[​](#hoepa_status "Direct link to hoepa_status")

* **Data Type**: `int`
* **Description:** Whether the covered loan is a high-cost mortgage
* **Values:**
	+ 1 - High-cost mortgage
	+ 2 - Not a high-cost mortgage
	+ 3 - Not applicable


### [total\_loan\_costs](#total_loan_costs)[​](#total_loan_costs "Direct link to total_loan_costs")

* **Data Type**: `float`
* **Description:** The amount, in dollars, of total loan costs
* **Values:**
	+ Varying values


### [total\_points\_and\_fees](#total_points_and_fees)[​](#total_points_and_fees "Direct link to total_points_and_fees")

* **Data Type**: `float`
* **Description:** The total points and fees, in dollars, charged in connection with the covered loan
* **Values:**
	+ Varying values


### [origination\_charges](#origination_charges)[​](#origination_charges "Direct link to origination_charges")

* **Data Type**: `float`
* **Description:** The total of all itemized amounts, in dollars, that are designated borrower-paid at or before closing
* **Values:**
	+ Varying values


### [discount\_points](#discount_points)[​](#discount_points "Direct link to discount_points")

* **Data Type**: `float`
* **Description:** The points paid, in dollars, to the creditor to reduce the interest rate
* **Values:**
	+ Varying values


### [lender\_credits](#lender_credits)[​](#lender_credits "Direct link to lender_credits")

* **Data Type**: `float`
* **Description:** The amount, in dollars, of lender credits
* **Values:**
	+ Varying values


### [loan\_term](#loan_term)[​](#loan_term "Direct link to loan_term")

* **Data Type**: `int`
* **Description:** The number of months after which the legal obligation will mature or terminate, or would have matured or terminated
* **Values:**
	+ Varying values


### [prepayment\_penalty\_term](#prepayment_penalty_term)[​](#prepayment_penalty_term "Direct link to prepayment_penalty_term")

* **Data Type**: `int`
* **Description:** The term, in months, of any prepayment penalty
* **Values:**
	+ Varying values


### [intro\_rate\_period](#intro_rate_period)[​](#intro_rate_period "Direct link to intro_rate_period")

* **Data Type**: `int`
* **Description:** The number of months, or proposed number of months in the case of an application, until the first date the interest rate may change after closing or account opening
* **Values:**
	+ Varying values


### [negative\_amortization](#negative_amortization)[​](#negative_amortization "Direct link to negative_amortization")

* **Data Type**: `int`
* **Description:** Whether the contractual terms include, or would have included, a term that would cause the covered loan to be a negative amortization loan
* **Values:**
	+ 1 - Negative amortization
	+ 2 - No negative amortization
	+ 1111 - Exempt


### [interest\_only\_payment](#interest_only_payment)[​](#interest_only_payment "Direct link to interest_only_payment")

* **Data Type**: `int`
* **Description:** Whether the contractual terms include, or would have included, interest-only payments
* **Values:**
	+ 1 - Interest-only payments
	+ 2 - No interest-only payments
	+ 1111 - Exempt


### [balloon\_payment](#balloon_payment)[​](#balloon_payment "Direct link to balloon_payment")

* **Data Type**: `int`
* **Description:** Whether the contractual terms include, or would have included, a balloon payment
* **Values:**
	+ 1 - Balloon payment
	+ 2 - No balloon payment
	+ 1111 - Exempt


### [other\_nonamortizing\_features](#other_nonamortizing_features)[​](#other_nonamortizing_features "Direct link to other_nonamortizing_features")

* **Data Type**: `int`
* **Description:** Whether the contractual terms include, or would have included, any term, other than those described in [Paragraphs 1003.4(a)(27)(i), (ii), and (iii)](https://www.consumerfinance.gov/policy-compliance/rulemaking/regulations/1003/4/#a-27) that would allow for payments other than fully amortizing payments during the loan term
* **Values:**
	+ 1 - Other non-fully amortizing features
	+ 2 - No other non-fully amortizing features
	+ 1111 - Exempt


### [property\_value](#property_value)[​](#property_value "Direct link to property_value")

* **Data Type**: `int`
* **Description:** The value of the property securing the covered loan or, in the case of an application, proposed to secure the covered loan, relied on in making the credit decision
* **Values:**
	+ Varying values; Rounded to the midpoint of the nearest $10,000 interval for which the reported value falls


### [construction\_method](#construction_method)[​](#construction_method "Direct link to construction_method")

* **Data Type**: `int`
* **Description:** Construction method for the dwelling
* **Values:**
	+ 1 - Site-built
	+ 2 - Manufactured home


### [occupancy\_type](#occupancy_type)[​](#occupancy_type "Direct link to occupancy_type")

* **Data Type**: `int`
* **Description:** Occupancy type for the dwelling
* **Values:**
	+ 1 - Principal residence
	+ 2 - Second residence
	+ 3 - Investment property


### [manufactured\_home\_secured\_property\_type](#manufactured_home_secured_property_type)[​](#manufactured_home_secured_property_type "Direct link to manufactured_home_secured_property_type")

* **Data Type**: `int`
* **Description:** Whether the covered loan or application is, or would have been, secured by a manufactured home and land, or by a manufactured home and not land
* **Values:**
	+ 1 - Manufactured home and land
	+ 2 - Manufactured home and not land
	+ 3 - Not applicable
	+ 1111 - Exempt


### [manufactured\_home\_land\_property\_interest](#manufactured_home_land_property_interest)[​](#manufactured_home_land_property_interest "Direct link to manufactured_home_land_property_interest")

* **Data Type**: `int`
* **Description:** The applicant’s or borrower’s land property interest in the land on which a manufactured home is, or will be, located
* **Values:**
	+ 1 - Direct ownership
	+ 2 - Indirect ownership
	+ 3 - Paid leasehold
	+ 4 - Unpaid leasehold
	+ 5 - Not applicable
	+ 1111 - Exempt


### [total\_units](#total_units)[​](#total_units "Direct link to total_units")

* **Data Type**: `int`
* **Description:** The number of individual dwelling units related to the property securing the covered loan or, in the case of an application, proposed to secure the covered loan
* **Values:**
	+ 1
	+ 2
	+ 3
	+ 4
	+ 5-24
	+ 25-49
	+ 50-99
	+ 100-149
	+ >149


### [applicant_age](#applicant_age)[​](#applicant_age "Direct link to ageapplicant")

* **Data Type**: `int`
* **Description:** The age of the applicant
* **Values:**
	+ <25
	+ 25-34
	+ 35-44
	+ 45-54
	+ 55-64
	+ 65-74
	+ >74
	+ 8888


### [multifamily\_affordable\_units](#multifamily_affordable_units)[​](#multifamily_affordable_units "Direct link to multifamily_affordable_units")

* **Data Type**: `int`
* **Description:** Reported values as a percentage, rounded to the nearest whole number, of the value reported for Total Units
* **Values:**
	+ Varying values


### [income](#income)[​](#income "Direct link to income")

* **Data Type**: `int`
* **Description:** The gross annual income, in thousands of dollars, relied on in making the credit decision, or if a credit decision was not made, the gross annual income relied on in processing the application
* **Values:**
	+ Varying values


### [debt\_to\_income\_ratio](#debt_to_income_ratio)[​](#debt_to_income_ratio "Direct link to debt_to_income_ratio")

* **Data Type**: `int`
* **Description:** The ratio, as a percentage, of the applicant’s or borrower’s total monthly debt to the total monthly income relied on in making the credit decision
* **Varying values; Ratios binned are:**
	+ <20%
	+ 20%-<30%
	+ 30%-<36%
	+ 36%
	+ 37%
	+ 38%
	+ 39%
	+ 40%
	+ 41%
	+ 42%
	+ 43%
	+ 44%
	+ 45%
	+ 46%
	+ 47%
	+ 48%
	+ 49%
	+ 50%-60%
	+ >60%
	+ NA
	+ Exempt


### [applicant\_credit\_score\_type](#applicant_credit_score_type)[​](#applicant_credit_score_type "Direct link to applicant_credit_score_type")

* **Data Type**: `int`
* **Description:** The name and version of the credit scoring model used to generate the credit score, or scores, relied on in making the credit decision
* **Values:**
	+ 1 - Equifax Beacon 5.0
	+ 2 - Experian Fair Isaac
	+ 3 - FICO Risk Score Classic 04
	+ 4 - FICO Risk Score Classic 98
	+ 5 - VantageScore 2.0
	+ 6 - VantageScore 3.0
	+ 7 - More than one credit scoring model
	+ 8 - Other credit scoring model
	+ 9 - Not applicable
	+ 1111 - Exempt


### [co-applicant\_credit\_score\_type](#co-applicant_credit_score_type)[​](#co-applicant_credit_score_type "Direct link to co-applicant_credit_score_type")

* **Data Type**: `int`
* **Description:** The name and version of the credit scoring model used to generate the credit score, or scores, relied on in making the credit decision
* **Values:**
	+ 1 - Equifax Beacon 5.0
	+ 2 - Experian Fair Isaac
	+ 3 - FICO Risk Score Classic 04
	+ 4 - FICO Risk Score Classic 98
	+ 5 - VantageScore 2.0
	+ 6 - VantageScore 3.0
	+ 7 - More than one credit scoring model
	+ 8 - Other credit scoring model
	+ 9 - Not applicable
	+ 10 - No co-applicant
	+ 1111 - Exempt


### [applicant\_ethnicity-1](#applicant_ethnicity-1)[​](#applicant_ethnicity-1 "Direct link to applicant_ethnicity-1")

* **Data Type**: `int`
* **Description:** Ethnicity of the applicant or borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino
	+ 3 - Information not provided by applicant in mail, internet, or telephone application
	+ 4 - Not applicable


### [applicant\_ethnicity-2](#applicant_ethnicity-2)[​](#applicant_ethnicity-2 "Direct link to applicant_ethnicity-2")

* **Data Type**: `int`
* **Description:** Ethnicity of the applicant or borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino


### [applicant\_ethnicity-3](#applicant_ethnicity-3)[​](#applicant_ethnicity-3 "Direct link to applicant_ethnicity-3")

* **Data Type**: `int`
* **Description:** Ethnicity of the applicant or borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino


### [applicant\_ethnicity-4](#applicant_ethnicity-4)[​](#applicant_ethnicity-4 "Direct link to applicant_ethnicity-4")

* **Data Type**: `int`
* **Description:** Ethnicity of the applicant or borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino


### [applicant\_ethnicity-5](#applicant_ethnicity-5)[​](#applicant_ethnicity-5 "Direct link to applicant_ethnicity-5")

* **Data Type**: `int`
* **Description:** Ethnicity of the applicant or borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino


### [co-applicant\_ethnicity-1](#co-applicant_ethnicity-1)[​](#co-applicant_ethnicity-1 "Direct link to co-applicant_ethnicity-1")

* **Data Type**: `int`
* **Description:** Ethnicity of the first co-applicant or co-borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino
	+ 3 - Information not provided by applicant in mail, internet, or telephone application
	+ 4 - Not applicable
	+ 5 - No co-applicant


### [co-applicant\_ethnicity-2](#co-applicant_ethnicity-2)[​](#co-applicant_ethnicity-2 "Direct link to co-applicant_ethnicity-2")

* **Data Type**: `int`
* **Description:** Ethnicity of the first co-applicant or co-borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino


### [co-applicant\_ethnicity-3](#co-applicant_ethnicity-3)[​](#co-applicant_ethnicity-3 "Direct link to co-applicant_ethnicity-3")

* **Data Type**: `int`
* **Description:** Ethnicity of the first co-applicant or co-borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino


### [co-applicant\_ethnicity-4](#co-applicant_ethnicity-4)[​](#co-applicant_ethnicity-4 "Direct link to co-applicant_ethnicity-4")

* **Data Type**: `int`
* **Description:** Ethnicity of the first co-applicant or co-borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino


### [co-applicant\_ethnicity-5](#co-applicant_ethnicity-5)[​](#co-applicant_ethnicity-5 "Direct link to co-applicant_ethnicity-5")

* **Data Type**: `int`
* **Description:** Ethnicity of the first co-applicant or co-borrower
* **Values:**
	+ 1 - Hispanic or Latino
	+ 11 - Mexican
	+ 12 - Puerto Rican
	+ 13 - Cuban
	+ 14 - Other Hispanic or Latino
	+ 2 - Not Hispanic or Latino


### [applicant\_ethnicity\_observed](#applicant_ethnicity_observed)[​](#applicant_ethnicity_observed "Direct link to applicant_ethnicity_observed")

* **Data Type**: `int`
* **Description:** Whether the ethnicity of the applicant or borrower was collected on the basis of visual observation or surname
* **Values:**
	+ 1 - Collected on the basis of visual observation or surname
	+ 2 - Not collected on the basis of visual observation or surname
	+ 3 - Not applicable


### [co-applicant\_ethnicity\_observed](#co-applicant_ethnicity_observed)[​](#co-applicant_ethnicity_observed "Direct link to co-applicant_ethnicity_observed")

* **Data Type**: `int`
* **Description:** Whether the ethnicity of the first co-applicant or co-borrower was collected on the basis of visual observation or surname
* **Values:**
	+ 1 - Collected on the basis of visual observation or surname
	+ 2 - Not collected on the basis of visual observation or surname
	+ 3 - Not applicable
	+ 4 - No co-applicant


### [applicant\_race-1](#applicant_race-1)[​](#applicant_race-1 "Direct link to applicant_race-1")

* **Data Type**: `int`
* **Description:** Race of the applicant or borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White
	+ 6 - Information not provided by applicant in mail, internet, or telephone application
	+ 7 - Not applicable


### [applicant\_race-2](#applicant_race-2)[​](#applicant_race-2 "Direct link to applicant_race-2")

* **Data Type**: `int`
* **Description:** Race of the applicant or borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White


### [applicant\_race-3](#applicant_race-3)[​](#applicant_race-3 "Direct link to applicant_race-3")

* **Data Type**: `int`
* **Description:** Race of the applicant or borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White


### [applicant\_race-4](#applicant_race-4)[​](#applicant_race-4 "Direct link to applicant_race-4")

* **Data Type**: `int`
* **Description:** Race of the applicant or borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White


### [applicant\_race-5](#applicant_race-5)[​](#applicant_race-5 "Direct link to applicant_race-5")

* **Data Type**: `int`
* **Description:** Race of the applicant or borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White


### [co-applicant\_race-1](#co-applicant_race-1)[​](#co-applicant_race-1 "Direct link to co-applicant_race-1")

* **Data Type**: `int`
* **Description:** Race of the first co-applicant or co-borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White
	+ 6 - Information not provided by applicant in mail, internet, or telephone application
	+ 7 - Not applicable
	+ 8 - No co-applicant


### [co-applicant\_race-2](#co-applicant_race-2)[​](#co-applicant_race-2 "Direct link to co-applicant_race-2")

* **Data Type**: `int`
* **Description:** Race of the first co-applicant or co-borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White


### [co-applicant\_race-3](#co-applicant_race-3)[​](#co-applicant_race-3 "Direct link to co-applicant_race-3")

* **Data Type**: `int`
* **Description:** Race of the first co-applicant or co-borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White


### [co-applicant\_race-4](#co-applicant_race-4)[​](#co-applicant_race-4 "Direct link to co-applicant_race-4")

* **Data Type**: `int`
* **Description:** Race of the first co-applicant or co-borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White


### [co-applicant\_race-5](#co-applicant_race-5)[​](#co-applicant_race-5 "Direct link to co-applicant_race-5")

* **Data Type**: `int`
* **Description:** Race of the first co-applicant or co-borrower
* **Values:**
	+ 1 - American Indian or Alaska Native
	+ 2 - Asian
	+ 21 - Asian Indian
	+ 22 - Chinese
	+ 23 - Filipino
	+ 24 - Japanese
	+ 25 - Korean
	+ 26 - Vietnamese
	+ 27 - Other Asian
	+ 3 - Black or African American
	+ 4 - Native Hawaiian or Other Pacific Islander
	+ 41 - Native Hawaiian
	+ 42 - Guamanian or Chamorro
	+ 43 - Samoan
	+ 44 - Other Pacific Islander
	+ 5 - White


### [applicant\_race\_observed](#applicant_race_observed)[​](#applicant_race_observed "Direct link to applicant_race_observed")

* **Data Type**: `int`
* **Description:** Whether the race of the applicant or borrower was collected on the basis of visual observation or surname
* **Values:**
	+ 1 - Collected on the basis of visual observation or surname
	+ 2 - Not collected on the basis of visual observation or surname
	+ 3 - Not applicable


### [co-applicant\_race\_observed](#co-applicant_race_observed)[​](#co-applicant_race_observed "Direct link to co-applicant_race_observed")

* **Data Type**: `int`
* **Description:** Whether the race of the first co-applicant or co-borrower was collected on the basis of visual observation or surname
* **Values:**
	+ 1 - Collected on the basis of visual observation or surname
	+ 2 - Not collected on the basis of visual observation or surname
	+ 3 - Not applicable
	+ 4 - No co-applicant


### [applicant\_sex](#applicant_sex)[​](#applicant_sex "Direct link to applicant_sex")

* **Data Type**: `int`
* **Description:** Sex of the applicant or borrower
* **Values:**
	+ 1 - Male
	+ 2 - Female
	+ 3 - Information not provided by applicant in mail, internet, or telephone application
	+ 4 - Not applicable
	+ 6 - Applicant selected both male and female


### [co-applicant\_sex](#co-applicant_sex)[​](#co-applicant_sex "Direct link to co-applicant_sex")

* **Data Type**: `int`
* **Description:** Sex of the first co-applicant or co-borrower
* **Values:**
	+ 1 - Male
	+ 2 - Female
	+ 3 - Information not provided by applicant in mail, internet, or telephone application
	+ 4 - Not applicable
	+ 5 - No co-applicant
	+ 6 - Co-applicant selected both male and female


### [applicant\_sex\_observed](#applicant_sex_observed)[​](#applicant_sex_observed "Direct link to applicant_sex_observed")

* **Data Type**: `int`
* **Description:** Whether the sex of the applicant or borrower was collected on the basis of visual observation or surname
* **Values:**
	+ 1 - Collected on the basis of visual observation or surname
	+ 2 - Not collected on the basis of visual observation or surname
	+ 3 - Not applicable


### [co-applicant\_sex\_observed](#co-applicant_sex_observed)[​](#co-applicant_sex_observed "Direct link to co-applicant_sex_observed")

* **Data Type**: `int`
* **Description:** Whether the sex of the first co-applicant or co-borrower was collected on the basis of visual observation or surname
* **Values:**
	+ 1 - Collected on the basis of visual observation or surname
	+ 2 - Not collected on the basis of visual observation or surname
	+ 3 - Not applicable
	+ 4 - No co-applicant


### [applicant\_age\_above\_62](#applicant_age_above_62)[​](#applicant_age_above_62 "Direct link to applicant_age_above_62")

* **Data Type**: `int`
* **Description:** Whether the applicant or borrower age is 62 or above
* **Values:**
	+ Yes
	+ No
	+ NA


### [co-applicant\_age](#co-applicant_age)[​](#co-applicant_age "Direct link to co-applicant_age")

* **Data Type**: `int`
* **Description:** The age, in years, of the first co-applicant or co-borrower
* **Varying values; Ages binned are:**
	+ < 25
	+ 25-34
	+ 35-44
	+ 45-54
	+ 55-64
	+ 65-74
	+ > 74
	+ 8888
	+ 9999


### [co-applicant\_age\_above\_62](#co-applicant_age_above_62)[​](#co-applicant_age_above_62 "Direct link to co-applicant_age_above_62")

* **Data Type**: `int`
* **Description:** Whether the co-applicant or co-borrower age is 62 or above
* **Values:**
	+ Yes
	+ No
	+ NA


### [submission\_of\_application](#submission_of_application)[​](#submission_of_application "Direct link to submission_of_application")

* **Data Type**: `int`
* **Description:** Whether the applicant or borrower submitted the application directly to the financial institution
* **Values:**
	+ 1 - Submitted directly to your institution
	+ 2 - Not submitted directly to your institution
	+ 3 - Not applicable
	+ 1111 - Exempt


### [initially\_payable\_to\_institution](#initially_payable_to_institution)[​](#initially_payable_to_institution "Direct link to initially_payable_to_institution")

* **Data Type**: `int`
* **Description:** Whether the obligation arising from the covered loan was, or, in the case of an application, would have been, initially payable to the financial institution
* **Values:**
	+ 1 - Initially payable to your institution
	+ 2 - Not initially payable to your institution
	+ 3 - Not applicable
	+ 1111 - Exempt


### [aus-1](#aus-1)[​](#aus-1 "Direct link to aus-1")

* **Data Type**: `int`
* **Description:** The automated underwriting system(s) (AUS) used by the financial institution to evaluate the application
* **Values:**
	+ 1 - Desktop Underwriter (DU)
	+ 2 - Loan Prospector (LP) or Loan Product Advisor
	+ 3 - Technology Open to Approved Lenders (TOTAL) Scorecard
	+ 4 - Guaranteed Underwriting System (GUS)
	+ 5 - Other
	+ 6 - Not applicable
	+ 7 - Internal Proprietary System
	+ 1111 - Exempt


### [aus-2](#aus-2)[​](#aus-2 "Direct link to aus-2")

* **Data Type**: `int`
* **Description:** The automated underwriting system(s) (AUS) used by the financial institution to evaluate the application
* **Values:**
	+ 1 - Desktop Underwriter (DU)
	+ 2 - Loan Prospector (LP) or Loan Product Advisor
	+ 3 - Technology Open to Approved Lenders (TOTAL) Scorecard
	+ 4 - Guaranteed Underwriting System (GUS)
	+ 5 - Other
	+ 7 - Internal Proprietary System


### [aus-3](#aus-3)[​](#aus-3 "Direct link to aus-3")

* **Data Type**: `int`
* **Description:** The automated underwriting system(s) (AUS) used by the financial institution to evaluate the application
* **Values:**
	+ 1 - Desktop Underwriter (DU)
	+ 2 - Loan Prospector (LP) or Loan Product Advisor
	+ 3 - Technology Open to Approved Lenders (TOTAL) Scorecard
	+ 4 - Guaranteed Underwriting System (GUS)
	+ 7 - Internal Proprietary System


### [aus-4](#aus-4)[​](#aus-4 "Direct link to aus-4")

* **Data Type**: `int`
* **Description:** The automated underwriting system(s) (AUS) used by the financial institution to evaluate the application
* **Values:**
	+ 1 - Desktop Underwriter (DU)
	+ 2 - Loan Prospector (LP) or Loan Product Advisor
	+ 3 - Technology Open to Approved Lenders (TOTAL) Scorecard
	+ 4 - Guaranteed Underwriting System (GUS)
	+ 7 - Internal Proprietary System


### [aus-5](#aus-5)[​](#aus-5 "Direct link to aus-5")

* **Data Type**: `int`
* **Description:** The automated underwriting system(s) (AUS) used by the financial institution to evaluate the application
* **Values:**
	+ 1 - Desktop Underwriter (DU)
	+ 2 - Loan Prospector (LP) or Loan Product Advisor
	+ 3 - Technology Open to Approved Lenders (TOTAL) Scorecard
	+ 4 - Guaranteed Underwriting System (GUS)
	+ 7 - Internal Proprietary System


### [denial\_reason-1](#denial_reason-1)[​](#denial_reason-1 "Direct link to denial_reason-1")

* **Data Type**: `int`
* **Description:** The principal reason, or reasons, for denial
* **Values:**
	+ 1 - Debt-to-income ratio
	+ 2 - Employment history
	+ 3 - Credit history
	+ 4 - Collateral
	+ 5 - Insufficient cash (downpayment, closing costs)
	+ 6 - Unverifiable information
	+ 7 - Credit application incomplete
	+ 8 - Mortgage insurance denied
	+ 9 - Other
	+ 10 - Not applicable


### [denial\_reason-2](#denial_reason-2)[​](#denial_reason-2 "Direct link to denial_reason-2")

* **Data Type**: `int`
* **Description:** The principal reason, or reasons, for denial
* **Values:**
	+ 1 - Debt-to-income ratio
	+ 2 - Employment history
	+ 3 - Credit history
	+ 4 - Collateral
	+ 5 - Insufficient cash (downpayment, closing costs)
	+ 6 - Unverifiable information
	+ 7 - Credit application incomplete
	+ 8 - Mortgage insurance denied
	+ 9 - Other


### [denial\_reason-3](#denial_reason-3)[​](#denial_reason-3 "Direct link to denial_reason-3")

* **Data Type**: `int`
* **Description:** The principal reason, or reasons, for denial
* **Values:**
	+ 1 - Debt-to-income ratio
	+ 2 - Employment history
	+ 3 - Credit history
	+ 4 - Collateral
	+ 5 - Insufficient cash (downpayment, closing costs)
	+ 6 - Unverifiable information
	+ 7 - Credit application incomplete
	+ 8 - Mortgage insurance denied
	+ 9 - Other


### [denial\_reason-4](#denial_reason-4)[​](#denial_reason-4 "Direct link to denial_reason-4")

* **Data Type**: `int`
* **Description:** The principal reason, or reasons, for denial
* **Values:**
	+ 1 - Debt-to-income ratio
	+ 2 - Employment history
	+ 3 - Credit history
	+ 4 - Collateral
	+ 5 - Insufficient cash (downpayment, closing costs)
	+ 6 - Unverifiable information
	+ 7 - Credit application incomplete
	+ 8 - Mortgage insurance denied
	+ 9 - Other


## Census fields produced by the U.S. Census Bureau and appended to public HMDA Data[​](#census-fields-produced-by-the-us-census-bureau-and-appended-to-public-hmda-data "Direct link to Census fields produced by the U.S. Census Bureau and appended to public HMDA Data")


### [tract\_population](#tract_population)[​](#tract_population "Direct link to tract_population")

* **Data Type**: `int`
* **Description:** Total population in tract
* **Values:**
	+ Varying values


### [tract\_minority\_population\_percent](#tract_minority_population_percent)[​](#tract_minority_population_percent "Direct link to tract_minority_population_percent")

* **Data Type**: `float`
* **Description:** Percentage of minority population to total population for tract, rounded to two decimal places
* **Values:**
	+ Varying values


### [ffiec\_msa\_md\_median\_family\_income](#ffiec_msa_md_median_family_income)[​](#ffiec_msa_md_median_family_income "Direct link to ffiec_msa_md_median_family_income")

* **Data Type**: `int`
* **Description:** FFIEC Median family income in dollars for the MSA/MD in which the tract is located (adjusted annually by FFIEC)
* **Values:**
	+ Varying values


### [tract\_to\_msa\_income\_percentage](#tract_to_msa_income_percentage)[​](#tract_to_msa_income_percentage "Direct link to tract_to_msa_income_percentage")

* **Data Type**: `float`
* **Description:** Percentage of tract median family income compared to MSA/MD median family income
* **Values:**
	+ Varying values


### [tract\_owner\_occupied\_units](#tract_owner_occupied_units)[​](#tract_owner_occupied_units "Direct link to tract_owner_occupied_units")

* **Data Type**: `int`
* **Description:** Number of dwellings, including individual condominiums, that are lived in by the owner
* **Values:**
	+ Varying values


### [tract\_one\_to\_four\_family\_homes](#tract_one_to_four_family_homes)[​](#tract_one_to_four_family_homes "Direct link to tract_one_to_four_family_homes")

* **Data Type**: `int`
* **Description:** Dwellings that are built to houses with fewer than 5 families
* **Values:**
	+ Varying values


### [tract\_median\_age\_of\_housing\_units](#tract_median_age_of_housing_units)[​](#tract_median_age_of_housing_units "Direct link to tract_median_age_of_housing_units")

* **Data Type**: `int`
* **Description:** Tract median age of homes
* **Values:**
	+ Varying values

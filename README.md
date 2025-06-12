# 🧾 Model Card: HMDA Redlining Detection Model

**Model Name:** `hmda_catboost_final_model.cbm`  
**Version:** 1.0  
**Owner:** Charlie Clark  
**Last Updated:** June 2025  
**Model Type:** Binary Classifier (CatBoost)  
**Target Variable:** `loan_approved` (1 = approved, 0 = denied)

## Intended Use
This model is intended for **research and auditing purposes only**. It estimates the likelihood of mortgage approval using HMDA data and is used to evaluate fairness and detect potential redlining patterns at geographic and demographic levels.

## Training Data
- **Data Source:** Public HMDA datasets (2018–2023), enriched with Census, Zillow, and FFIEC data.
- **Sample Size:** ~72 million applications
- **Features:** Income, property value, loan amount, lender behavior, tract-level demographics, race, etc.

## Performance
| Metric               | Value      |
|----------------------|------------|
| Accuracy             | ~87.2%     |
| AUC-ROC              | ~0.86      |
| F1 Score             | ~0.92      |
| DPD (Score Gap)      | 0.14–0.20  |
| EOD (Approval Gap)   | 0.15–0.22  |

Performance is monitored across racial subgroups to evaluate disparities.

## Fairness and Explainability
- **DPD/EOD Metrics** computed per census tract and race
- **SHAP values** used to identify most influential features per group
- **Counterfactual Analysis** performed by flipping race while holding other variables constant

## Limitations
- Race is explicitly used for fairness audits and is not intended for real-world prediction.
- The model may reproduce systemic biases present in historical data.
- Predictions are **not** to be used for loan decisioning or regulatory enforcement without human oversight.

## Ethical Considerations
- This project is intended to support accountability in lending institutions.
- Outputs should be interpreted in collaboration with compliance experts or policymakers.
- Special care should be taken when presenting group-level results in public or legal contexts.

## License
[MIT License](LICENSE)

## Citation
> Clark, C. (2025). *Redlining Detection in HMDA Using Explainable ML*. Private Capstone Project.

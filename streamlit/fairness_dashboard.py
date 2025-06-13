import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go 
import os



# Snowflake Connection
conn = snowflake.connector.connect(
    user=st.secrets.SNOWFLAKE_USER,
    password=st.secrets.SNOWFLAKE_PASSWORD,
    account=st.secrets.SNOWFLAKE_ACCOUNT,
    warehouse=st.secrets.SNOWFLAKE_WAREHOUSE,
    database="HMDA_REDLINING",
    schema="SOURCE"
)

# Load dataframes
fairness_df = pd.read_sql("SELECT * FROM hmda_redlining.source.mart_hmda_fairness_scoring", conn)
overall_score_df = pd.read_sql("select * from hmda_redlining.source.metrics_overall_model_score", conn)
year_score_df = pd.read_sql("select * from hmda_redlining.source.metrics_model_score_by_year", conn)
calibration_df = pd.read_sql("select * from metrics_calibration_by_group", conn)
approval_year_df = pd.read_sql("select * from hmda_redlining.source.metrics_annual_approval_rate_by_group", conn)
score_distro_df = pd.read_sql("select * from hmda_redlining.source.metrics_predicted_score_distribution", conn)
gap_df = pd.read_sql("select * from hmda_redlining.source.mart_hmda_redlining_detection", conn)
shap_df = pd.read_sql("select feature, mean_shap_value from hmda_redlining.source.shap_values_log where experiment_name ilike 'Final_experiment_validation_full_set'", conn)
counter_df = pd.read_sql("select * from hmda_redlining.source.race_counterfactual_results", conn)
cf_geo_df = pd.read_sql("select * from hmda_redlining.source.mart_hmda_race_counterfactual_redlining", conn)

st.title("HMDA Redlining Model")

with st.expander("📘 Executive Summary"):
    st.markdown("""
    **Project Title:** _Detecting Potential Redlining in Mortgage Lending Using Machine Learning_

    **Objective:**  
    This project leverages machine learning to detect potential racial bias in mortgage approval decisions across the United States. Using Home Mortgage Disclosure Act (HMDA) data from 2018–2023 and additional socioeconomic and geographic datasets, the model aims to identify patterns consistent with redlining.

    **Methodology:**  
    A CatBoost classification model was trained on over 70 million applications. The model predicts the likelihood of a loan being approved using features including income, property value, loan characteristics, borrower demographics, and tract-level socioeconomic indicators.

    Key components include:
    - Preprocessing pipelines (dbt + Python)
    - Stratified sampling and fair feature engineering
    - SHAP explainability and score distribution visualizations
    - Tract-level score gaps and denial disparities
    - Counterfactual race-flip simulations to evaluate disparate impact

    **Findings:**
    - Certain minority groups, particularly Black and Latinx applicants, experience systematically lower predicted scores and approval rates in specific tracts.
    - Gaps between model-predicted approval probabilities and actual approval outcomes suggest potential institutional disparities that may warrant further policy review.
    - Counterfactual simulations show that changing only the applicant’s race significantly changes approval likelihood in specific regions.

    **Limitations:**  
    - Model is trained on historical data which may reflect embedded systemic biases.
    - Race is used as a sensitive feature to test for discrimination; it is not used in production decisions.
    - Counterfactuals assume only race changes, holding all other features constant, which may oversimplify real-world applicant experiences.

    **Next Steps:**  
    - Integrate with regulatory review tools
    - Expand to analyze lender-level behavioral trends
    - Collaborate with fair lending audit teams to interpret tract-level results
    """)

st.header("Model Performance")

with st.expander("ℹ️ What do these model performance metrics mean?"):
    st.markdown("""
    ### Model Performance Definitions and Interpretation
                
    **🟢 AUC-ROC** (Area Under the Receiver Operating Characteristics Curve)
    - Reflects the model's ability to rank approved loans better than denied ones
    - Higher is better (1.0 = perfect, 0.5 = random guessing)
                
    **🟡 F1 Score**
    - The harmonic mean of precision and recall.  It rewards models that correctly identify 
    positive cases without too many false alarms
    - Higher is better; 1.0 is max
                
    **🔵 Accuracy**
    - Accuracy is the proportion of correct predictions overall.  Be cautious when classes are 
    imbalanced, it may look high even if the model misses important cases
    """)

st.subheader("Overall Model Performance")
st.dataframe(data=overall_score_df, use_container_width=True)
st.subheader("Model Performance By Year")
st.dataframe(data=year_score_df, use_container_width=True)


st.header("🏛️ Fairness Dashboard")

with st.expander("ℹ️ What do these fairness metrics mean?"):
    st.markdown("""
    ### Fairness Metric Definitions and Interpretation

    **🔷 ACTUAL_APPROVAL_RATES**  
    The true approval rate observed for each group in the real-world data.

    **🔷 TRUE_POSITIVE_RATE (TPR)**  
    The proportion of actually approved applicants that the model correctly predicts as approved (aka recall).

    **🔷 FALSE_POSITIVE_RATE (FPR)**  
    The proportion of denied applicants that the model incorrectly predicts as approved.

    ---

    ### 📉 Statistical Significance in Fairness Metrics

    **What does "statistically significant" mean?**  
    A gap is considered **statistically significant** if the 95% confidence intervals of a group and the reference group (White applicants) **do not overlap**.

    This means we're 95% confident the observed difference is **not due to random chance**.

    **Why does it matter?**  
    Highlighting significance helps identify which disparities are likely to persist in the population — not just in the sample.

    > 🟥 Red bars indicate gaps that are statistically significant.  
    > 🟩 Green bars indicate no statistically significant difference from the reference group.
                
    ### 🎯 Calibration Curves

    **What is it?**  
    Calibration curves compare the model’s **predicted probability** of approval with the **actual approval rate** observed in the data.

    Each point on the curve represents a **bin of applicants** with similar predicted scores (e.g., 0.6–0.7).  
    The X-axis shows the model’s average predicted approval score in that bin.  
    The Y-axis shows the actual observed approval rate.

    **How to interpret it:**  
    - A **perfectly calibrated** model falls on the diagonal line (`y = x`)  
    - Points **below** the line → the model is **overconfident** (predicts higher approval than observed)  
    - Points **above** the line → the model is **underconfident**

    **Why does it matter?**  
    Calibration helps us understand whether the model is making **well-grounded predictions** across racial groups — not just whether it's accurate, but whether it's fair in how it estimates risk.
    """)

# Select metric and year
metric = st.selectbox("Select Metric", ["TRUE_POSITIVE_RATE", "FALSE_POSITIVE_RATE", "ACTUAL_APPROVAL_RATES"])
year = st.selectbox("Select Year", sorted(fairness_df["ACTIVITY_YEAR"].unique()))

# Filter
df_filtered = fairness_df[fairness_df["ACTIVITY_YEAR"] == year]

# Chart
fig = px.bar(
    df_filtered,
    x="APPLICANT_DERIVED_RACIAL_CATEGORY",
    y=metric,
    error_y=df_filtered["EOD_CI_UPPER"] - df_filtered["TRUE_POSITIVE_RATE"], 
    error_y_minus=df_filtered["TRUE_POSITIVE_RATE"] - df_filtered["EOD_CI_LOWER"], 
    color="APPLICANT_DERIVED_RACIAL_CATEGORY",
    title=f"{metric} by Racial Category ({year})",
    labels={metric: metric.replace("_", " ").title()}
)
st.plotly_chart(fig, use_container_width=True)

# Gap analysis
st.subheader("TPR & FPR Gap Analysis")

gap_metric = st.selectbox(
    "Select Gap Metric", 
    ["TPR_GAP", "FPR_GAP"]
)
significance_flag_col = "IS_TPR_GAP_SIGNIFICANT" if gap_metric == "TPR_GAP" else "IS_FPR_GAP_SIGNIFICANT"
hover_text = df_filtered.apply(
    lambda row: f"""
    Racial Group: {row['APPLICANT_DERIVED_RACIAL_CATEGORY']}<br>
    {gap_metric.replace('_', ' ')}: {row[gap_metric]:.3f}<br>
     Significance: {'Yes (statistically significant)' if row[significance_flag_col] else 'No'}
    """, axis=1
)

gap_fig = px.bar(
    df_filtered, 
    x="APPLICANT_DERIVED_RACIAL_CATEGORY", 
    y=gap_metric, 
    color="IS_TPR_GAP_SIGNIFICANT" if gap_metric == "TRP_GAP" else "IS_FPR_GAP_SIGNIFICANT", 
    color_discrete_map={True: "red", False: "green"}, 
    title=f"{gap_metric.replace('_', ' ')} by Racial Category ({year})", 
    labels={gap_metric: gap_metric.replace("_", " ").title()}, 
    hover_data={"APPLICANT_DERIVED_RACIAL_CATEGORY": False, gap_metric: True}
)

gap_fig.update_traces(hovertemplate=hover_text)

gap_fig.update_layout(
    yaxis_title=gap_metric.replace("_", " ").title(), 
    xaxis_title="Racial Category", 
    showlegend=False, 
    uniformtext_minsize=8, 
    uniformtext_mode="hide"
)

st.plotly_chart(gap_fig, use_container_width=True)

# Precision, Recall, and F1 Score by Group
st.subheader("Precision, Recall, and F1 Score by Group")

prf_metric = st.selectbox(
    "Select Metric", 
    ["PRECISION_RATE", "TRUE_POSITIVE_RATE", "F1_SCORE"]
)

prf_fig = px.bar(
    df_filtered,
    x="APPLICANT_DERIVED_RACIAL_CATEGORY",
    y=prf_metric,
    color="APPLICANT_DERIVED_RACIAL_CATEGORY",
    title=f"{prf_metric.replace('_', ' ').title()} by Racial Category ({year})",
    labels={prf_metric: prf_metric.replace("_", " ").title()}
)

st.plotly_chart(prf_fig, use_container_width=True)

# Calibration by group
st.subheader("Calibration Curve by Group")

calibration_df = calibration_df.sort_values(by="AVG_PREDICTED_SCORE")
cal_fig = px.line(
    calibration_df,
    x="AVG_PREDICTED_SCORE",
    y="ACTUAL_APPROVAL_RATE",
    color="APPLICANT_DERIVED_RACIAL_CATEGORY",
    markers=True,
    title="Calibration Curves by Racial Group",
    labels={
        "AVG_PREDICTED_SCORE": "Predicted Probability",
        "ACTUAL_APPROVAL_RATE": "Actual Approval Rate"
    }
)

# Add ideal reference line (y = x)
cal_fig.add_shape(
    type="line",
    line_dash="dash",
    line_color="gray",
    x0=0, y0=0, x1=1, y1=1
)

cal_fig.update_layout(legend_title_text="Racial Group")

st.plotly_chart(cal_fig, use_container_width=True, key="calibration_all_groups")

st.header("Distribution and Exposure Metrics")

with st.expander("ℹ️ What do these metrics mean?"):
    st.markdown("""
    ### 📈 Approval Rate by Year
    - Shows how actual approval rates vary over time for different groups, helping 
    to contextualize disparities as persistent or improving.
    
    ---
                
    ### 📈 Predicted Approval Rate by Year
    - Reveals how the model predicted by race each year
                
    ---

    ### 📈 Cumulative Distribution of Predicted Scores (CDF)

    **What it shows:**  
    This chart displays the cumulative proportion of applicants in a given racial group who receive a predicted score *less than or equal to* each value on the X-axis.

    - The X-axis shows the model's predicted approval score.
    - The Y-axis shows the cumulative percentage of applicants at or below that score.

    **How to interpret it:**
    - A **steeper curve on the left** means more applicants are being scored *low*.
    - A **flatter curve** indicates that applicants are more evenly spread or skewed toward higher scores.

    **Why it matters:**  
    Even if a model is calibrated and fair at the group level, it may still **concentrate low scores** in certain groups — limiting their opportunity to qualify under threshold-based approval rules.

    > Example:  
    If 80% of Group A falls below a score of 0.7, but only 40% of Group B does, then Group A is far less likely to be approved.
    """)

approval_year_df = approval_year_df.sort_values(by="ACTIVITY_YEAR")

grp_app_fig = px.line(
    approval_year_df, 
    x='ACTIVITY_YEAR', 
    y='ACTUAL_APPROVAL_RATE', 
    color='RACE', 
    markers=True, 
    title='Actual Approval Rate by Group and Year'
)

st.plotly_chart(grp_app_fig, use_container_width=True)

grp_app_fig = px.line(
    approval_year_df, 
    x='ACTIVITY_YEAR', 
    y='PREDICTED_APPROVAL_RATE', 
    color='RACE', 
    markers=True, 
    title='Predicted Approval Rate by Group and Year'
)

st.plotly_chart(grp_app_fig, use_container_width=True)



group = st.selectbox("Select Group", sorted(score_distro_df["APPLICANT_DERIVED_RACIAL_CATEGORY"].unique()))
group_df = score_distro_df[score_distro_df["APPLICANT_DERIVED_RACIAL_CATEGORY"] == group]

cdf_df = group_df.groupby("AVG_SCORE").agg(total=("N_IN_BIN", "sum")).sort_index().cumsum()
cdf_df["percentile"] = cdf_df["total"] / cdf_df["total"].max()

dist_fig = px.line(
    cdf_df.reset_index(),
    x="AVG_SCORE", y="percentile",
    title=f"Cumulative Distribution of Predicted Scores: {group}",
    labels={"percentile": "Cumulative Proportion", "SCORE": "Predicted Score"}
)
st.plotly_chart(dist_fig, use_container_width=True)

st.header("🗺️ Geographic Redlining Patterns by Census Tract")

gap_df = gap_df.dropna(subset=["LATITUDE", "LONGITUDE"])
gap_df = gap_df[gap_df["RACE"].isin(["Latinx", "Black", "AAPI"])]

with st.expander("ℹ️ What does this map show?"):
    st.markdown("""
    This map displays census tracts where minority applicants had lower approval rates than predicted by the race-aware model.
    
    **Larger gaps (model_vs_actual_gap)** suggest potential redlining:
    - 🔶 A positive gap means the model predicted higher approval likelihood than actually occurred.
    - 🔴 Census tracts flagged as **possible** or **strong redlining** are colored accordingly.

    """)

min_apps = st.slider("Minimum number of applications per census tract", 1, 500, 30)

filtered_gap_df = gap_df[gap_df["TOTAL_APPS"] >= min_apps]

map_fig = px.scatter_mapbox(
    filtered_gap_df,
    lat="LATITUDE",
    lon="LONGITUDE",
    color="MODEL_VS_ACTUAL_GAP",
    color_continuous_scale="Reds",
    size="TOTAL_APPS",
    opacity=0.8,
    hover_name="CENSUS_TRACT",
    hover_data={
        "RACE": True,
        "ACTUAL_APPROVAL_RATE": ":.2f",
        "PREDICTED_APPROVAL_RATE": ":.2f",
        "MODEL_VS_ACTUAL_GAP": ":.2f",
        "SCORE_DPD": ":.2f",
        "SCORE_EOD": ":.2f",
        "STATE_NAME": True,
        "CITY_NAME": True
    },
    zoom=4,
    height=600
)
map_fig.update_layout(mapbox_style="carto-positron", margin={"r":0,"t":0,"l":0,"b":0})
st.plotly_chart(map_fig, use_container_width=True)

st.subheader("📋 Top Tracts with Highest Model vs Actual Approval Gaps")

with st.expander("ℹ️ What does this table show?"):
    st.markdown("""
    ### Understanding the Table of Highest-Risk Census Tracts

    This table lists census tracts where the **gap between the model-predicted approval rate** and the **actual approval rate** is largest.

    - **Predicted Approval Rate:** What the model thinks should happen, accounting for race and other features.
    - **Actual Approval Rate:** What actually happened in the historical HMDA data.
    - **Model vs Actual Gap:** A large **negative gap** may signal **potential redlining** — where applicants were less likely to be approved than the model anticipated.
    - **Score DPD / Score EOD:** Compares approval likelihoods against White applicants. Values **< 0.8** are redlining indicators.
    
    Use this table to explore which areas may require further policy or compliance review.
    """)

top_tracts_df = filtered_gap_df.sort_values("MODEL_VS_ACTUAL_GAP", ascending=False).head(40)
st.dataframe(top_tracts_df[[
    "CENSUS_TRACT", "STATE_NAME", "CITY_NAME", "RACE",
    "ACTUAL_APPROVAL_RATE", "PREDICTED_APPROVAL_RATE",
    "MODEL_VS_ACTUAL_GAP", "SCORE_DPD", "SCORE_EOD"
]], use_container_width=True)

st.header("Explainability and Accountability")

st.subheader("📈 SHAP (SHapely Additive exPlanations) Feature Importance")

with st.expander("ℹ️ What does this graph mean?"): 
    st.markdown("""
    ### Understanding the Chart of SHAP Feature Importance
    
    SHAP (SHapley Additive exPlanations) values quantify how much each feature contributes to a 
    prediction.  This chart shows the average magnitude of those contributions across all validation 
    set predictions.  Features with higher mean SHAP values had a larger overall influence on the model's 
    decisions.
    """)

shap_sorted_df = shap_df.sort_values("MEAN_SHAP_VALUE", ascending=True)

shap_fig = px.bar(
    shap_sorted_df, 
    x="MEAN_SHAP_VALUE", 
    y="FEATURE", 
    orientation="h", 
    title="Mean SHAP Values by Feature (Validation Subset)", 
    labels={"MEAN_SHAP_VALUE": "Mean Absolute SHAP Value", "FEATURE": "Feature"}, 
    hover_data=["MEAN_SHAP_VALUE"]
)

shap_fig.update_layout(yaxis_title="", xaxis_title="Mean |SHAP| Value", height=800)
st.plotly_chart(shap_fig, use_container_width=True)

st.header("Counterfactual Analysis")

with st.expander("ℹ️ Counterfactual Race Flip Analysis"): 
    st.markdown("""
    These visualizations explore how an applicant's **predicted score** changes when we hypothetically change their recorded race — a technique known as **counterfactual analysis**.

    **Heatmap – Average Score Change by Race Flip**  
    Each cell shows the **average change in predicted score** when applicants of one race are re-scored as if they belonged to another race.  
    - **Rows** represent the applicant’s **original recorded race**.  
    - **Columns** represent the **new race** used during the hypothetical scoring.  
    - **Positive values** (red) indicate increased scores after the race flip; **negative values** (blue) indicate decreased scores.

    This chart helps identify **systematic disparities** in how the model treats applicants based on race.

    **Bar Chart – Average Score Change by New Race**  
    This summarizes the average impact of each race **if all applicants were treated as that race**.  
    For example, a +0.03 score change for "White" means that applicants, on average, saw a 3 percentage point increase when hypothetically scored as White.

    These analyses highlight how **model predictions shift under race reassignment**, providing transparency into how race may implicitly affect model outcomes — even in a race-aware model.
    """)

st.subheader("Average Score by Race Flip")
counter_pivot_df = counter_df.groupby(['ORIGINAL_RACE', 'NEW_RACE'])['SCORE_CHANGE'].mean().reset_index()
cf_heatmap_df = counter_pivot_df.pivot(index='ORIGINAL_RACE', columns='NEW_RACE', values='SCORE_CHANGE')

cf_heat_fig = px.imshow(
    cf_heatmap_df, 
    text_auto=True, 
    color_continuous_scale='RdBu', 
    zmin=-0.1, 
    zmax=0.1
)

st.plotly_chart(cf_heat_fig, use_container_width=True)

st.subheader("Average Score Change by New Race")
avg_score_by_new_race = counter_df.groupby("NEW_RACE")['SCORE_CHANGE'].mean().reset_index()

cf_bar_fig = px.bar(
    avg_score_by_new_race, 
    x="NEW_RACE", 
    y="SCORE_CHANGE", 
    color='NEW_RACE', 
    labels={"SCORE_CHANGE": "Avg Score Change"}, 
    title="Effect of Being Classified as a Different Race"
)

cf_bar_fig.update_layout(showlegend=False, yaxis=dict(range=[-0.1, 0.1]))
st.plotly_chart(cf_bar_fig, use_container_width=True)

st.subheader("Label Flip Rate by Race Flip")

with st.expander("ℹ️ About Label Flip Rate"):
    st.markdown("""
    This chart shows how often a counterfactual race change causes the model's predicted outcome to flip
    across the approval threshold (e.g., from denial to approval or vice versa). 
    
    **Why this matters**: If changing the applicant's race flips the model decision frequently, this may indicate 
    potential racial sensitivity or bias in the model’s learned decision boundary.

    **Interpretation**:
    - A high flip rate suggests the model is treating applicants differently based on race — especially near the decision threshold.
    - This doesn't necessarily imply unfairness by itself, but may warrant further investigation when combined with other evidence.
    """)

threshold = 0.5
counter_df['ORIGINAL_LABEL'] = (counter_df['PREVIOUS_SCORE'] >= threshold).astype(int)
counter_df['COUNTERFACTUAL_LABEL'] = (counter_df['COUNTERFACTUAL_SCORE'] >= threshold).astype(int)

counter_df['LABEL_FLIP'] = (counter_df['ORIGINAL_LABEL'] != counter_df['COUNTERFACTUAL_LABEL']).astype(int)

flip_rate_df = counter_df.groupby(['ORIGINAL_RACE', 'NEW_RACE'])['LABEL_FLIP'].agg(['sum', 'count']).reset_index()
flip_rate_df['FLIP_RATE'] = flip_rate_df['sum'] / flip_rate_df['count']

flip_heatmap_df = flip_rate_df.pivot(index='ORIGINAL_RACE', columns='NEW_RACE', values='FLIP_RATE')

flip_heatmap_fig = px.imshow(
    flip_heatmap_df, 
    text_auto=".2%", 
    color_continuous_scale='Purples', 
    labels=dict(color='Flip Rate'), 
    title="Label Flip Rate by Race Flip", 
    zmin=0, 
    zmax=flip_heatmap_df.values.max()
)

st.plotly_chart(flip_heatmap_fig, use_container_width=True)

with st.expander("ℹ️ What does this chart show?"):
    st.markdown("""
    This chart shows how often changing the applicant's race (while holding all other features constant) causes the model's prediction to flip across the approval threshold of 0.5.

    - Each bar represents a pair of races: the applicant's **original race** and the **new race** used in the counterfactual.
    - Higher flip rates may suggest that race plays a critical role in the model’s decision-making process for that applicant group.
    - Use this to identify which race transitions are most sensitive and may require further audit.

    Note: This is **not** definitive evidence of discrimination, but rather a diagnostic tool to investigate fairness concerns.
    """)

sorted_flip_df = flip_rate_df.sort_values("FLIP_RATE", ascending=False).copy()
sorted_flip_df['RACE_FLIP_PAIR'] = sorted_flip_df['ORIGINAL_RACE'] + " ➝ " + sorted_flip_df['NEW_RACE']
sorted_flip_df['FLIP_RATE_LABEL'] = sorted_flip_df['FLIP_RATE'].apply(lambda x: f"{x:.2%}")

flip_bar_fig = px.bar(
    sorted_flip_df,
    x="RACE_FLIP_PAIR",
    y="FLIP_RATE",
    color="ORIGINAL_RACE",
    text="FLIP_RATE_LABEL",
    labels={"FLIP_RATE": "Flip Rate", "RACE_FLIP_PAIR": "Race Flip Pair"},
    title="Label Flip Rate by Race Flip Pair",
)

flip_bar_fig.update_traces(textposition="outside")
flip_bar_fig.update_layout(
    xaxis_tickangle=-45,
    yaxis=dict(range=[0, flip_rate_df['FLIP_RATE'].max() * 1.1])
)

st.plotly_chart(flip_bar_fig, use_container_width=True)

st.subheader("📍 Counterfactual Score Change by Census Tract")

with st.expander("ℹ️ What does this map show?"):
    st.markdown("""
    This map shows the **average change in predicted score** for mortgage applicants
    when their race is hypothetically changed (counterfactual analysis).

    - Each dot represents a **census tract**.
    - The color indicates the **average change in score** when race is flipped.
    - Use the filters below to explore different **race transitions** across regions.
    
    **Positive values** indicate that applicants would have received higher predicted scores
    under the new race; **negative values** indicate the opposite.
    """)

default_cf_orig = "Black"
default_cf_new = "White"
orig_race = st.selectbox(
    "Original Race", 
    sorted(cf_geo_df['ORIGINAL_RACE'].unique()), 
    index=sorted(cf_geo_df['ORIGINAL_RACE'].unique()).index(default_cf_orig)
)
new_race = st.selectbox(
    "New Race", 
    sorted(cf_geo_df['NEW_RACE'].unique()),
    index=sorted(cf_geo_df['NEW_RACE'].unique()).index(default_cf_new)
)
cf_min_apps = st.slider("Minimum Applications per Census Tract", min_value=1, max_value=500, value=30, step=5)

cf_geo_filtered_df = cf_geo_df[
    (cf_geo_df['ORIGINAL_RACE'] == orig_race) & 
    (cf_geo_df['NEW_RACE'] == new_race) & 
    (cf_geo_df['TOTAL_APPS'] >= cf_min_apps) & 
    (cf_geo_df['AVG_SCORE_CHANGE'].notnull())
]

score_map = px.scatter_mapbox(
    cf_geo_filtered_df,
    lat="LATITUDE",
    lon="LONGITUDE",
    color="AVG_SCORE_CHANGE",
    size="TOTAL_APPS",
    color_continuous_scale="RdBu",
    range_color=[-0.1, 0.1],
    mapbox_style="carto-positron",
    zoom=4,
    hover_name="CENSUS_TRACT",
    hover_data={
        "COUNTY_CODE": True,
        "TOTAL_APPS": True,
        "AVG_PREV_SCORE": ":.3f",
        "AVG_COUNTERFACTUAL_SCORE": ":.3f",
        "AVG_SCORE_CHANGE": ":.3f"
    },
    title=f"Avg Counterfactual Score Change: {orig_race} ➝ {new_race}"
)

st.plotly_chart(score_map, use_container_width=True)


st.subheader("📊 Distribution of Application Counts")

with st.expander("ℹ️ What does this chart show?"):
    st.markdown("""
    This histogram displays the distribution of **total mortgage applications** 
    per census tract for the selected counterfactual race flip.

    - Tracts with a **higher number of applications** are considered more statistically reliable.
    - Use this chart to **inform your minimum threshold setting** above the map.
    - Areas with very few applications may show large score changes due to limited data.

    Filtering out low-application tracts helps focus on patterns that are **more stable and generalizable**.
    """)

hist_fig = px.histogram(
    cf_geo_df[
        (cf_geo_df["ORIGINAL_RACE"] == orig_race) &
        (cf_geo_df["NEW_RACE"] == new_race) &
        cf_geo_df["TOTAL_APPS"].notnull()
    ],
    x="TOTAL_APPS",
    nbins=50,
    title="Histogram of Total Applications per Census Tract",
    labels={"TOTAL_APPS": "Number of Applications"}
)

st.plotly_chart(hist_fig, use_container_width=True)
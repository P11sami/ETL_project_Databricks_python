# Databricks notebook source
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from scipy.stats import pearsonr

plt.style.use('ggplot')
sns.set_palette("husl")

race_results = spark.table("ddca_exc4.gold.race_results_by_date")

# 1. Top 5 Drivers by Race Wins
driver_wins = (race_results
    .filter(F.col("race_position") == "1")
    .groupBy("driver_forename", "driver_surname")
    .agg(F.count("*").alias("wins"))
    .orderBy(F.desc("wins"))
    .limit(5)
    .toPandas())

driver_wins["driver_name"] = driver_wins["driver_forename"] + " " + driver_wins["driver_surname"]

fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x="wins", y="driver_name", data=driver_wins, ax=ax)
ax.set_title("Top 5 Drivers by Race Wins")
ax.set_xlabel("Number of Wins")
ax.set_ylabel("Driver")
plt.tight_layout()
display(fig)
plt.close(fig)

# 2. Top 5 Drivers by Qualifying Wins
qualifying_wins = (race_results
    .filter(F.col("qualifying_position") == "1")
    .groupBy("driver_forename", "driver_surname")
    .agg(F.count("*").alias("qualifying_wins"))
    .orderBy(F.desc("qualifying_wins"))
    .limit(5)
    .toPandas())

qualifying_wins["driver_name"] = qualifying_wins["driver_forename"] + " " + qualifying_wins["driver_surname"]

fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x="qualifying_wins", y="driver_name", data=qualifying_wins, ax=ax)
ax.set_title("Top 5 Drivers by Qualifying Wins")
ax.set_xlabel("Number of Pole Positions")
ax.set_ylabel("Driver")
plt.tight_layout()
display(fig)
plt.close(fig)

# 3. Top 5 Constructors by Win Rate
constructor_stats = (race_results
    .groupBy("constructor_name")
    .agg(
        F.countDistinct(F.year("date")).alias("seasons_raced"),
        F.sum(F.when(F.col("race_position") == "1", 1).otherwise(0)).alias("wins")
    )
    .withColumn("win_rate", F.col("wins") / F.col("seasons_raced"))
    .orderBy(F.desc("win_rate"))
    .limit(5)
    .toPandas())

fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x="win_rate", y="constructor_name", data=constructor_stats, ax=ax)
ax.set_title("Top 5 Constructors by Win Rate (Wins/Seasons Raced)")
ax.set_xlabel("Win Rate (Wins per Season)")
ax.set_ylabel("Constructor")
plt.tight_layout()
display(fig)
plt.close(fig)

# 4. Top 5 Circuits by DNF Rate
circuit_dnf_stats = (race_results
    .groupBy("circuit_name")
    .agg(
        F.count("*").alias("total_races"),
        F.sum(F.when(F.col("race_position") == "DNF", 1).otherwise(0)).alias("dnfs")
    )
    .withColumn("dnf_rate", F.col("dnfs") / F.col("total_races"))
    .orderBy(F.desc("dnf_rate"))
    .limit(5)
    .toPandas())

fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x="dnf_rate", y="circuit_name", data=circuit_dnf_stats, ax=ax)
ax.set_title("Top 5 Circuits by DNF Rate (DNFs/Total Races)")
ax.set_xlabel("DNF Rate")
ax.set_ylabel("Circuit")
plt.tight_layout()
display(fig)
plt.close(fig)

# 5. Correlation Between Driver Nationality and Race Wins
def interpret_correlation(r):
    """Interpret correlation coefficient strength"""
    abs_r = abs(r)
    if abs_r >= 0.8:
        return "Very strong relationship"
    elif abs_r >= 0.6:
        return "Strong relationship"
    elif abs_r >= 0.4:
        return "Moderate relationship"
    elif abs_r >= 0.2:
        return "Weak relationship"
    else:
        return "Very weak or no relationship"

nationality_stats = (race_results
    .filter(F.col("race_position") == "1")
    .groupBy("driver_nationality")
    .agg(F.count("*").alias("wins"))
    .join(
        race_results.groupBy("driver_nationality")
                   .agg(F.countDistinct(F.concat("driver_forename", "driver_surname")).alias("driver_count")),
        on="driver_nationality",
        how="outer"
    )
    .fillna(0)
    .orderBy(F.desc("wins"))
    .toPandas())

nationality_stats["wins_per_driver"] = nationality_stats["wins"] / nationality_stats["driver_count"]
nationality_stats = nationality_stats[nationality_stats["driver_count"] > 0]

corr, p_value = pearsonr(nationality_stats["driver_count"], nationality_stats["wins"])

fig, ax = plt.subplots(figsize=(12, 8))
scatter = sns.scatterplot(
    x="driver_count", 
    y="wins", 
    size="wins_per_driver",
    sizes=(50, 300),
    hue="driver_nationality",
    data=nationality_stats,
    ax=ax,
    legend="brief"
)

ax.set_title(f"Driver Nationality vs Race Wins\nPearson Correlation: {corr:.2f} (p-value: {p_value:.4f})")
ax.set_xlabel("Number of Drivers from Nationality")
ax.set_ylabel("Total Race Wins")
ax.grid(True)

top_nationalities = nationality_stats.nlargest(5, "wins")
for _, row in top_nationalities.iterrows():
    ax.text(
        row["driver_count"] + 0.5, 
        row["wins"] + 0.5, 
        row["driver_nationality"],
        fontsize=10,
        ha='left'
    )

z = np.polyfit(nationality_stats["driver_count"], nationality_stats["wins"], 1)
p = np.poly1d(z)
plt.plot(
    nationality_stats["driver_count"], 
    p(nationality_stats["driver_count"]), 
    "r--",
    label="Trendline"
)

plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
display(fig)
plt.close(fig)

print("Correlation Interpretation:")
print(f"Pearson Correlation: {corr:.2f}")
print(f"p-value: {p_value:.4f}")
print(f"Interpretation: {interpret_correlation(corr)}")

# 6. Wins per Nationality Normalized by Driver Count
nationality_wins = (race_results
    .filter(F.col("race_position") == "1")
    .groupBy("driver_nationality")
    .agg(F.count("*").alias("wins"))
    .orderBy(F.desc("wins"))
    .limit(10)
    .toPandas())

total_drivers = (race_results
    .groupBy("driver_nationality")
    .agg(F.countDistinct(F.concat("driver_forename", "driver_surname")).alias("total_drivers"))
    .toPandas())

nationality_stats_normalized = pd.merge(
    nationality_wins,
    total_drivers,
    on="driver_nationality",
    how="left"
).fillna(0)

nationality_stats_normalized["wins_per_driver"] = nationality_stats_normalized["wins"] / nationality_stats_normalized["total_drivers"]

fig, ax = plt.subplots(figsize=(12, 6))
sns.barplot(
    x="wins_per_driver", 
    y="driver_nationality", 
    data=nationality_stats_normalized.sort_values("wins_per_driver", ascending=False),
    ax=ax
)
ax.set_title("Wins per Driver by Nationality (Normalized)")
ax.set_xlabel("Average Wins per Driver")
ax.set_ylabel("Nationality")
plt.tight_layout()
display(fig)
plt.close(fig)

display(nationality_stats.sort_values("wins", ascending=False))
display(nationality_stats_normalized.sort_values("wins_per_driver", ascending=False))
"""@bruin
name: report.johnson_nestedness_vs_degree
image: python:3.13

depends:
  - experiment.johnson_nestedness

description: |
  Generate scatter plot of local Johnson nestedness (g_norm) vs node degree,
  faceted by community and colored by bipartite set.
  Also generates a summary CSV with global nestedness metrics per community.

@bruin"""

import pandas as pd
import numpy as np
import duckdb
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr, spearmanr
import os

DB_PATH = os.environ.get("BRUIN_DUCKDB_PATH", "us_pipeline.duckdb")
FIGURES_DIR = os.environ.get("BRUIN_FIGURES_DIR", "outputs/figures/johnson_nestedness")

os.makedirs(FIGURES_DIR, exist_ok=True)

# Load data
con = duckdb.connect(DB_PATH, read_only=True)
df = con.execute("SELECT * FROM experiment.johnson_nestedness").fetchdf()
con.close()

print(f"Loaded {len(df)} rows from experiment.johnson_nestedness")
print(f"Communities: {df['community'].unique().tolist()}")

# 1. Summary table
summary_rows = []
for comm, group in df.groupby("community"):
    summary_rows.append({
        "Community": comm,
        "Nodes": len(group),
        "Set 0 (late)": (group["set"] == 0).sum(),
        "Set 1 (early)": (group["set"] == 1).sum(),
        "g_norm": group["g_norm"].iloc[0],
        "g_raw": group["g_raw"].iloc[0],
        "g_conf": group["g_conf"].iloc[0],
        "Mean Degree": group["degree"].mean(),
        "Mean Local g_norm": group["local_g_norm"].mean(),
    })

summary_df = pd.DataFrame(summary_rows)
summary_path = os.path.join(FIGURES_DIR, "johnson_summary.csv")
summary_df.to_csv(summary_path, index=False)
print(f"\nSummary saved to {summary_path}")
print(summary_df.to_string(index=False))

# 2. Main scatter plot: Degree vs Local Nestedness
communities = sorted(df["community"].unique())
n_comm = len(communities)

fig, axes = plt.subplots(2, n_comm, figsize=(8 * n_comm, 12))
if n_comm == 1:
    axes = axes.reshape(-1, 1)

fig.suptitle(
    "Johnson Nestedness: Local g_norm vs Degree by Community",
    fontsize=18,
    fontweight="bold",
    y=1.01,
)

colors = {0: "#3498db", 1: "#e74c3c"}
set_labels = {0: "Set 0 (Late-stage)", 1: "Set 1 (Early-stage)"}

for idx, comm in enumerate(communities):
    comm_df = df[df["community"] == comm]
    g_norm_global = comm_df["g_norm"].iloc[0]

    # Row 1: Histogram of local g_norm by set
    ax1 = axes[0, idx]
    for set_id in [0, 1]:
        set_data = comm_df[comm_df["set"] == set_id]["local_g_norm"]
        ax1.hist(
            set_data, bins=30, alpha=0.6, color=colors[set_id],
            label=set_labels[set_id], edgecolor="black", linewidth=0.5,
        )
    ax1.set_title(f"{comm}\nLocal g_norm Distribution (global g_norm={g_norm_global:.4f})", fontweight="bold", fontsize=12)
    ax1.set_xlabel("Local g_norm", fontsize=11)
    ax1.set_ylabel("Frequency", fontsize=11)
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)

    # Row 2: Scatter plot — degree vs local nestedness
    ax2 = axes[1, idx]
    for set_id in [0, 1]:
        set_data = comm_df[comm_df["set"] == set_id]
        ax2.scatter(
            set_data["degree"], set_data["local_g_norm"],
            alpha=0.5, s=40, color=colors[set_id], label=set_labels[set_id],
            edgecolors="white", linewidths=0.3,
        )

    # Correlation annotation
    valid = comm_df.dropna(subset=["degree", "local_g_norm"])
    if len(valid) > 2:
        r_p, p_p = pearsonr(valid["degree"], valid["local_g_norm"])
        r_s, p_s = spearmanr(valid["degree"], valid["local_g_norm"])
        ax2.annotate(
            f"Pearson r={r_p:.3f} (p={p_p:.1e})\nSpearman rho={r_s:.3f} (p={p_s:.1e})",
            xy=(0.02, 0.98), xycoords="axes fraction",
            fontsize=8, va="top", ha="left",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", alpha=0.8),
        )

    ax2.set_title(f"{comm}\nDegree vs Local Nestedness", fontweight="bold", fontsize=12)
    ax2.set_xlabel("Node Degree", fontsize=11)
    ax2.set_ylabel("Local g_norm", fontsize=11)
    ax2.legend(fontsize=9)
    ax2.grid(True, alpha=0.3)

plt.tight_layout()
fig_path = os.path.join(FIGURES_DIR, "johnson_nestedness_vs_degree.png")
plt.savefig(fig_path, dpi=300, bbox_inches="tight")
plt.close()
print(f"\nScatter plot saved to {fig_path}")

# 3. Bar chart: g_norm comparison across communities
fig2, ax = plt.subplots(figsize=(8, 5))
comm_names = summary_df["Community"].tolist()
g_norms = summary_df["g_norm"].tolist()
bar_colors = ["#1f77b4", "#ff7f0e", "#2ca02c"][:n_comm]

bars = ax.bar(range(n_comm), g_norms, color=bar_colors, edgecolor="black", linewidth=1.2, alpha=0.85)
ax.axhline(y=1.0, color="red", linestyle="--", alpha=0.7, linewidth=1.5, label="Random expectation (g=1)")
ax.set_xticks(range(n_comm))
ax.set_xticklabels(comm_names, fontsize=11)
ax.set_ylabel("g_norm (Normalized Nestedness)", fontsize=12, fontweight="bold")
ax.set_title("Johnson Nestedness (g_norm) Across Communities", fontsize=14, fontweight="bold")
ax.legend(fontsize=10)
ax.grid(True, alpha=0.3, axis="y")

for bar, val in zip(bars, g_norms):
    ax.text(bar.get_x() + bar.get_width() / 2.0, bar.get_height(),
            f"{val:.4f}", ha="center", va="bottom", fontsize=10, fontweight="bold")

plt.tight_layout()
fig2_path = os.path.join(FIGURES_DIR, "johnson_gnorm_comparison.png")
plt.savefig(fig2_path, dpi=300, bbox_inches="tight")
plt.close()
print(f"Bar chart saved to {fig2_path}")

# 4. Correlation analysis printout
print("\n" + "=" * 80)
print("CORRELATION ANALYSIS: DEGREE vs LOCAL NESTEDNESS")
print("=" * 80)

for comm in communities:
    comm_df = df[df["community"] == comm]
    print(f"\n{comm}:")

    valid = comm_df.dropna(subset=["degree", "local_g_norm"])
    if len(valid) > 2:
        r_p, p_p = pearsonr(valid["degree"], valid["local_g_norm"])
        r_s, p_s = spearmanr(valid["degree"], valid["local_g_norm"])
        print(f"  Overall - Pearson r={r_p:.4f} (p={p_p:.6f}), Spearman rho={r_s:.4f} (p={p_s:.6f})")

    for set_id, set_name in set_labels.items():
        set_data = comm_df[comm_df["set"] == set_id].dropna(subset=["degree", "local_g_norm"])
        if len(set_data) > 2:
            r_p, p_p = pearsonr(set_data["degree"], set_data["local_g_norm"])
            r_s, p_s = spearmanr(set_data["degree"], set_data["local_g_norm"])
            print(f"  {set_name} - Pearson r={r_p:.4f} (p={p_p:.6f}), Spearman rho={r_s:.4f} (p={p_s:.6f})")

print("\nReport complete.")

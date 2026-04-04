This repository contains the official implementation of Faithful KG-RAG, a robust LLM-augmented framework designed to defend Knowledge-Graph Question Answering (KGQA) systems against fine-grained adversarial poisoning attacks (e.g., edge poisoning).

📖 Table of Contents
Overview

Repository Structure

Installation

Usage & Commands

Framework Pipeline

Main Results

Citation

💡 Overview
Knowledge graphs are highly vulnerable to localized data corruption, which can easily derail multi-hop reasoning models. Faithful KG-RAG acts as an advanced protective layer. It treats the Large Language Model (LLM) as an expert adjudicator that operates exclusively on a purified, path-centric scan of the Knowledge Graph (KG).

By executing a strict "triple-filtering" mechanism, our framework not only predicts the correct answer with high fidelity but also attributes the errors back to the source, detecting and removing the poisoned facts to heal the underlying graph.

📁 Repository Structure
.
├── data/                      # Directory for datasets (e.g., clean and poisoned WebQSP)
│   ├── webqsp_clean/
│   └── webqsp_poisoned/
├── src/                       # Core source code for the Faithful KG-RAG pipeline
│   ├── path_generation/       # Offline relation-path hypothesis generation (e.g., via RoG)
│   ├── grounding/             # Relation-constrained grounding modules
│   ├── purification/          # Path-focused context purification algorithms
│   └── attribution/           # Poisoned facts detection and graph healing logic
├── requirements.txt           # Python dependencies and package versions
├── predict_answer.py          # Main execution script for the end-to-end pipeline
├── evaluate.py                # Script to compute Hits@1, F1, Precision, and Recall
└── README.md                  # Project documentation

⚙️ Installation
1. Clone the repository:
git clone https://github.com/zhangye213/Triple-filtering-using-large-language-model-against-adversarial-attack-to-knowledge-graph.git

2. Create a virtual environment (Recommended):
conda create -n faithful-kgrag python=3.9
conda activate faithful-kgrag

3. Install the required dependencies:
pip install -r requirements.txt

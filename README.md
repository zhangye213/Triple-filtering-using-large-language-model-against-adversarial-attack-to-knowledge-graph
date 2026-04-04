# Triple-Filtering LLM against Adversarial Attack to Knowledge Graph

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/release/python-380/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This repository implements **Faithful KG-RAG**, a robust framework designed to defend Knowledge-Graph Question Answering (KGQA) systems against edge-poisoning adversarial attacks. It utilizes an LLM as an expert adjudicator to filter poisoned triples and heal the knowledge graph.

---

## 📂 Repository Structure

* **`src/`**: Core logic for the triple-filtering pipeline.
  * `grounding.py`: Performs relation-constrained grounding on polluted graphs.
  * `purification.py`: Filters off-path triples to create a focused context.
  * `attribution.py`: Identifies and flags poisoned triples.
* **`scripts/`**: Utility scripts for data processing.
* **`predict_answer.py`**: The main entry point for running the defense and getting answers.
* **`evaluate.py`**: Performance evaluation (Hits@1, F1, Precision/Recall).
* **`requirements.txt`**: List of required Python packages.

---

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone [https://github.com/zhangye213/Triple-filtering-using-large-language-model-against-adversarial-attack-to-knowledge-graph.git](https://github.com/zhangye213/Triple-filtering-using-large-language-model-against-adversarial-attack-to-knowledge-graph.git)
   cd Triple-filtering-using-large-language-model-against-adversarial-attack-to-knowledge-graph

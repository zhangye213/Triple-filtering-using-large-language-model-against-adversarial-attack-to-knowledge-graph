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

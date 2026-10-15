# 🤖 Advanced Semantic Search & Hybrid Query Engine

This standalone module provides **vector embeddings, semantic search, and hybrid querying** over tech news articles.

---

## 📁 Module Contents

* **`semantic_search.ipynb`**: Interactive notebook demonstrating:
  1. Dense text embedding generation (`title + summary`).
  2. Pairwise cosine similarity & nearest-neighbor recommendations (`top_similar_articles`).
  3. Pure vector semantic search (`find_similar_articles(query_text, top_k=5)`).
  4. SQL + vector hybrid search (`hybrid_search(...)`).
  5. Generating the enriched dataset `ai_articles_enriched.csv`.
* **`data/embeddings/article_embeddings.npy`**: Pre-computed 384-dimensional dense vector embeddings matrix for all 750 articles.

---

## 🚀 How to Run

Open and execute `semantic_search.ipynb` in VS Code / Jupyter.

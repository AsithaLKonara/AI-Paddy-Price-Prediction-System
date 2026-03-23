# 🚀 Google Colab Training Guide

Follow these steps to train your AI Paddy Price Prediction model using Google Colab.

## 📦 Required Files
Before starting, ensure you have the following files from this repository:
1.  `paddy_price_training.ipynb` (The Jupyter Notebook)
2.  `data/dataset.jsonl` (The cleaned dataset)

---

## 🛠 Step-by-Step Instructions

### 1. Open Google Colab
Go to [colab.research.google.com](https://colab.research.google.com/) and sign in with your Google account.

### 2. Upload the Notebook
*   Click on **File** > **Upload notebook**.
*   Select `paddy_price_training.ipynb` from your local project folder.

### 3. Upload the Dataset
*   On the left sidebar of Colab, click the **Folder icon** (Files).
*   Create a folder named `data` (Right-click > New folder).
*   Right-click the `data` folder and select **Upload**.
*   Select `dataset.jsonl` from your local `data/` folder.
*   *Wait for the upload to complete (blue circle at the bottom left).*

### 4. Run the Training
*   Click on **Runtime** > **Run all**.
*   The notebook will automatically:
    *   Install/import necessary libraries.
    *   Load and validate the data.
    *   Perform EDA on Maha/Yala seasons.
    *   Generate features and train the XGBoost model.
    *   Display performance metrics and charts.

---

## 💡 Pro-Tips
*   **Persistent Storage**: If you want to save your models, consider mounting your Google Drive by clicking the "Mount Drive" icon in the Files pane and updating the paths in the notebook.
*   **GPU Acceleration**: While XGBoost is very fast on CPUs, you can enable GPU acceleration in **Runtime** > **Change runtime type** > **T4 GPU** for even faster training on larger datasets.

---

## 🧪 Troubleshooting
*   **FileNotFoundError**: Ensure the `dataset.jsonl` is inside a folder named `data` in the Colab file explorer.
*   **ModuleNotFoundError**: If a library is missing, add a cell at the top with `!pip install library_name`.

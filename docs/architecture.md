# A Vision for the Analyst-Driven Underwriting Workflow

The primary goal of this system is to empower the underwriting analyst. We move from a manual, error-prone spreadsheet process to a robust, transparent, and efficient code-driven workflow. The architecture is designed to give the analyst full control and deep insight at every step.

This diagram provides a detailed look inside the Underwriting architecture. It shows the specific components and tools that the analyst directly interacts with, the data stores that support them, and key external integrations.

```mermaid
graph TD
    subgraph " "
        A["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-user-tie Analyst</div>"]
    end

    subgraph "Presentation Layer (The Analyst's Toolkit)"
        direction LR
        B["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-desktop Streamlit App</div><div style='font-size: 0.9em; margin-top: 5px;'>Manual & API Workflows</div>"]
        C["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-book dbt Docs</div><div style='font-size: 0.9em; margin-top: 5px;'>Data Lineage & Dictionary</div>"]
        D["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-flask JupyterLab</div><div style='font-size: 0.9em; margin-top: 5px;'>Ad-hoc SQL Analysis</div>"]
    end
    
    subgraph "Orchestration & Data Processing"
        direction LR
        E["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-play-circle Python Pipelines</div><div style='font-size: 0.9em; margin-top: 5px;'>ingest_statements.py<br/>transform_statements.py</div>"]
        F["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-recycle dbt Core</div><div style='font-size: 0.9em; margin-top: 5px;'>SQL-based Transformations</div>"]
    end

    subgraph "Data & External Services"
        direction LR
        G["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-database Data Warehouse</div><div style='font-size: 0.9em; margin-top: 5px;'>DuckDB & Delta Lake</div>"]
        H["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-server Mock API</div><div style='font-size: 0.9em; margin-top: 5px;'>Flask Service</div>"]
        I["<div style='font-size: 1.1em; font-weight: bold;'>fa:fa-cogs Taktile API</div><div style='font-size: 0.9em; margin-top: 5px;'>Decision Engine</div>"]
    end

    A -- "Uses" --> B & C & D
    A -- "Models Decisions In" --> I
    
    B -- "Triggers" --> E
    B -- "Sends/Receives Data" --> I

    E -- "Gets Data From" --> H
    E -- "Writes Data To" --> G
    E -- "Runs" --> F
    
    F -- "Reads/Writes To" --> G
    
    B -- "Reads Data From" --> G
    C -- "Reads Metadata From" --> G
    D -- "Queries Data From" --> G

    style B fill:#DDEBF7,stroke:#333,stroke-width:2px
    style C fill:#DDEBF7,stroke:#333,stroke-width:2px
    style D fill:#DDEBF7,stroke:#333,stroke-width:2px
```

# Finance Analytics Portfolio

Focused finance and accounting analytics projects that demonstrate decision modeling, scenario comparison, business interpretation, and stakeholder-facing recommendations.

This repository is intentionally organized as a portfolio collection, not as an installable package. Each project should stand alone with a concise README, cleaned shareable artifacts, and enough context to explain the financial decision logic.

## Projects

| Project | Techniques | Portfolio Signal |
| --- | --- | --- |
| [Capital Budgeting Case Study](projects/capital-budgeting-case-study) | NPV, equivalent annual cost, tax shield modeling, option comparison | Financial modeling, long-horizon decision analysis, executive recommendation framing |
| [Month-End Close ETL](projects/month-end-close-etl) | Config-driven AWS Glue jobs, business-day Airflow orchestration, processor file normalization, NetSuite journal-entry generation, KMS-encrypted outbound | Finance data engineering: close automation with controllership-grade outputs |

## Repository Pattern

Each project should keep a simple structure:

```text
projects/<project-name>/
├── README.md
├── data/
├── diagrams/
└── docs/
```

Use `data/` only for cleaned, shareable summary artifacts. Keep raw Office exports, classroom reports, workbook metadata, and files with personal metadata out of the public repo.

## Future Project Ideas

- Budget variance analysis
- Forecasting and scenario planning
- Working capital or cash conversion analysis
- Unit economics and contribution margin modeling
- Finance operations dashboards

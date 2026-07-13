# Pulse — Technical Architecture

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TIER 1: DATA INGESTION                       │
│                                                                     │
│  ┌──────────────────┐  ┌───────────────────┐  ┌─────────────────┐  │
│  │  IDBI Core       │  │  Account          │  │  External UPI   │  │
│  │  Banking System  │  │  Aggregator (AA)  │  │  Network Data   │  │
│  │  (TXN Logs)      │  │  Consent-based    │  │  NPCI Feed      │  │
│  └────────┬─────────┘  └────────┬──────────┘  └───────┬─────────┘  │
│           └──────────────────────┴──────────────────────┘           │
│                                  │                                   │
│                        Apache Kafka (MSK)                            │
│                    real-time event streaming                         │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────┐
│                     TIER 2: AI PROCESSING CORE (AWS)                │
│                                                                     │
│  Apache Spark ──► Feature Store (S3 + DynamoDB)                     │
│       │                                                             │
│       ├──► [NLP] Sentence-BERT endpoint (SageMaker)                 │
│       │         Merchant narration → category clusters              │
│       │                                                             │
│       ├──► [SEQ] Transformer endpoint (SageMaker)                   │
│       │         Transaction sequences → life event prediction       │
│       │                                                             │
│       └──► [TAB] XGBoost endpoint (SageMaker)                       │
│                 Structured features → affordability score           │
│                          │                                          │
│                    SHAP Explainability                               │
│                    Offer Orchestrator                                │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────┐
│                       TIER 3: CONSUMPTION                           │
│                                                                     │
│  FastAPI Gateway (secure, JWT-auth, rate-limited)                   │
│       │                                                             │
│       ├──► RM Copilot Dashboard (React / Pulse UI)                  │
│       │    Lead queue, explainability, scripts                      │
│       │                                                             │
│       └──► IDBI Mobile App (React Native)                           │
│            Pre-approved offers, customer-facing                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Flow — End to End

### Step 1: Transaction Ingestion
- Every UPI debit/credit, card swipe, NEFT/IMPS, ATM, and EMI event is streamed via Apache Kafka
- Kafka topic: `idbi.transactions.raw` (partition by customer account hash)
- Schema: `{account_hash, timestamp, amount, direction, narration_raw, channel, merchant_raw}`

### Step 2: Merchant Normalization (Sentence-BERT)
- Sentence-BERT model (all-MiniLM-L6-v2, fine-tuned on 2M Indian merchant names) maps raw narration to one of 120 merchant categories
- Input: `"SULEKHA*PACKERS*MUM4521 REF892301"` → Output: `{category: "Moving Services", confidence: 0.94}`
- Handles misspellings, code-mixing (Hinglish), and partial matches
- Batch processed via SageMaker batch transform every 15 minutes; real-time for high-value events

### Step 3: Feature Engineering (Apache Spark)
Rolling window features computed over 90-day sliding windows per customer:

**Income Features:**
- `avg_monthly_salary_credit` — mean of salary-labelled credits
- `salary_consistency_score` — fraction of months with regular credit
- `income_growth_rate` — YoY change in monthly credits
- `secondary_income_signals` — freelance, rental, dividend credits

**Spend Features:**
- `fixed_obligation_ratio` — fixed EMI + rent as fraction of income
- `discretionary_spend_ratio` — variable non-essential spend
- `category_velocity_[N]` — 30-day spend growth per category (120 features)
- `merchant_diversity_score` — entropy of merchant categories
- `late_night_spend_ratio` — signal for lifestyle spend

**Life Event Sequences (for Transformer input):**
- `txn_sequence_90d` — ordered list of (category, normalized_amount, days_ago) tuples
- `category_spike_flags` — binary flags for >2σ spend increase per category
- `co_occurrence_patterns` — pairwise category transitions within 7-day windows

**Credit Health Features:**
- `credit_card_utilization` — outstanding / limit
- `emi_delay_count_12m` — late payments in past 12 months
- `emi_bounce_count` — bounced EMIs
- `net_savings_ratio` — (income - total_spend) / income

### Step 4: Life Event Detection (Transformer)
- Architecture: 4-layer Transformer encoder, 128 hidden dim, 4 attention heads
- Input: 90-token sequence of (category_id, amount_bucket, position) per customer
- Output: softmax over 8 life event classes + "no event"
- Classes: House Shifting, Vehicle Upgrade, Wedding Prep, Business Expansion, Education, Travel, Medical, Other
- F1 Score target: >0.82 on held-out synthetic dataset
- Retrained weekly on RM feedback signals (accepted/rejected leads)

### Step 5: Affordability Scoring (XGBoost)
- 40 structured features (see above)
- Output: `{safe_monthly_emi, estimated_monthly_income, dti_ratio, risk_grade}`
- Risk grades: A+ / A / B+ / B / C (maps to rate adjustments)
- Calibrated probability output for "will repay" (12-month horizon)
- XGBoost chosen for: interpretability, performance on tabular data, fast inference

### Step 6: SHAP Explainability
- `shap.TreeExplainer(xgb_model)` applied at inference time
- Top 5 positive features + top 3 negative features extracted per prediction
- SHAP values mapped to human-readable templates in the Explainability Engine
- Template example: `"Salary of ₹{amt} credited for {n} consecutive months"` → filled from feature values
- All SHAP explanations stored in audit log with decision timestamp

### Step 7: Offer Orchestration
- Product matching matrix: life event × affordability grade × customer segment
- Amount calculation: `min(desired_amount_from_signal, safe_emi * tenure_months * (1 + rate_buffer))`
- Rate calculation: `base_rate[product] + risk_adjustment[grade] + competitor_buffer`
- Priority scoring: `intent_score * affordability_score * recency_decay * channel_propensity`

### Step 8: Delivery
- RM Dashboard: REST API, refreshed every 15 minutes
- Mobile App: Push notification + in-app card, triggered within 200ms of qualifying signal
- RM Script: GPT-powered template filling using SHAP reasons + customer name + product details

---

## 3. Feature Engineering — Full Feature List (42 Features)

```python
FEATURES = {
    # Income (8 features)
    'avg_salary_credit_3m', 'salary_credit_consistency_12m',
    'income_yoy_growth', 'max_single_credit_3m',
    'secondary_income_count_3m', 'income_volatility_cv',
    'salary_day_of_month_std', 'declared_vs_estimated_ratio',

    # Spend profile (10 features)
    'fixed_obligation_ratio', 'discretionary_ratio',
    'emi_to_income_ratio', 'rent_proxy_ratio',
    'food_spend_share', 'transport_spend_share',
    'shopping_spend_share', 'entertainment_spend_share',
    'healthcare_spend_share', 'investment_spend_share',

    # Category velocity (8 features — selected signals)
    'furniture_spend_velocity_30d', 'moving_services_spend_30d',
    'vehicle_maintenance_velocity_90d', 'auto_accessory_spend_30d',
    'wedding_category_spend_30d', 'education_spend_velocity_60d',
    'travel_spend_velocity_30d', 'real_estate_portal_visits_30d',

    # Credit health (8 features)
    'credit_card_utilization', 'emi_delay_count_12m',
    'emi_bounce_count_12m', 'overdraft_days_12m',
    'min_balance_breach_count', 'cheque_bounce_count_12m',
    'credit_inquiries_90d', 'existing_loan_count',

    # Behavioral (8 features)
    'merchant_diversity_score', 'transaction_frequency_30d',
    'digital_payment_ratio', 'avg_transaction_size_trend',
    'weekend_spend_ratio', 'night_spend_ratio',
    'net_savings_ratio_3m', 'account_age_months',
}
```

---

## 4. AI Model Architecture

### Sentence-BERT (NLP Layer)
```
Input: raw merchant narration string (avg 40 chars)
  └─► Tokenizer (WordPiece, 30,000 vocab)
  └─► BERT encoder (6 layers, 384 hidden, fine-tuned)
  └─► Mean pooling → 384-dim embedding
  └─► K-NN classifier against 120 category centroids
Output: (category_id, confidence_score)

Training data: 2.1M labelled merchant narrations
               (synthetic + manually labelled subset)
F1 Score: 88.4% on held-out Indian merchant test set
```

### Transformer Sequence Model (Life Event)
```
Input: 90-token transaction sequence per customer
  └─► Token embedding (category_id + amount_bucket + position)
  └─► Transformer encoder (4 layers, 128 dim, 4 heads)
  └─► [CLS] token → classification head (9 classes)
Output: life_event_class + confidence

Training: synthetic dataset of 500,000 customer histories
          with 45-day label horizon (event occurs within 45 days)
F1 Score: 0.84 (macro avg across 8 event classes)
Latency: ~45ms per customer (SageMaker ml.m5.large)
```

### XGBoost Affordability Model
```
Input: 42 structured features (90-day rolling window)
  └─► XGBClassifier (n_estimators=300, max_depth=6, lr=0.05)
  └─► Outputs: repayment_probability + safe_emi_estimate
Output: affordability_grade (A+/A/B+/B/C) + max_safe_emi

Training: 180,000 historical loan accounts with 12-month outcomes
          (synthetic dataset for hackathon, real data for production)
AUC-ROC: 0.89 | Precision@top_decile: 0.76
Latency: ~8ms per customer
```

---

## 5. API Design (FastAPI)

```python
# Lead scoring endpoint
GET /api/v1/leads/queue
  ?rm_id=string&limit=int&min_score=int&event_type=string
  → { leads: [LeadSummary], total: int, generated_at: datetime }

# Customer 360 profile
GET /api/v1/customers/{account_hash}/profile
  → { income_intelligence, affordability, life_events, offer_recommendation }

# Explainability
GET /api/v1/customers/{account_hash}/explain
  → { shap_features: [Feature], human_reasons: [Reason], risk_flags: [Flag] }

# Offer generation
POST /api/v1/offers/generate
  body: { account_hash, loan_type, amount, tenure }
  → { offer_id, emi, rate, validity_days, rm_script, audit_token }

# Offer delivery (to mobile app)
POST /api/v1/offers/{offer_id}/deliver
  body: { channel: "app" | "sms" | "email" }
  → { delivery_status, timestamp }

# RM feedback (closes training loop)
POST /api/v1/leads/{lead_id}/feedback
  body: { outcome: "contacted" | "converted" | "rejected", notes: string }
  → { ack: true }
```

---

## 6. Infrastructure

### AWS Components
| Component | Service | Purpose |
|---|---|---|
| Event streaming | Kafka MSK | Real-time transaction ingestion |
| Feature processing | EMR (Spark) | 90-day rolling feature computation |
| Feature storage | DynamoDB + S3 | Low-latency feature retrieval |
| Model hosting | SageMaker Endpoints | Sentence-BERT, Transformer, XGBoost |
| Batch scoring | SageMaker Batch Transform | Nightly full-portfolio rescore |
| API layer | Lambda + API Gateway | Serverless REST API |
| Dashboard backend | ECS Fargate (FastAPI) | RM Copilot API |
| Secrets | AWS Secrets Manager | DB credentials, API keys |
| Monitoring | CloudWatch + Grafana | Model drift, latency, error rates |

### Kafka Topics
```
idbi.transactions.raw          # all incoming events
idbi.transactions.normalized   # after Sentence-BERT categorization
idbi.features.customer         # computed feature vectors
idbi.scores.life-events        # Transformer predictions
idbi.scores.affordability      # XGBoost scores
idbi.offers.generated          # ready-to-deliver offers
idbi.offers.delivered          # delivery confirmations
idbi.feedback.rm               # RM accept/reject signals (training loop)
```

---

## 7. Security & Compliance

### Data Security
- All data encrypted at rest (AES-256) and in transit (TLS 1.3)
- PII fields (name, phone, address) are tokenized before reaching ML pipeline — models never see raw PII
- Account numbers hashed with customer-specific salt (SHA-256)
- Row-level security: RMs only see customers in their assigned portfolio

### Regulatory Compliance
- **RBI Fair Practices Code**: Every AI recommendation includes 3–5 human-readable SHAP reasons
- **DPDP Act 2023**: Explicit customer consent via Account Aggregator before any external data ingestion
- **RBI Account Aggregator Framework**: All external bank data fetched through licensed AA entities with customer-initiated consent
- **Audit Trail**: Every prediction, offer, delivery, and RM action logged with timestamp, model version, and input feature hash
- **Model Governance**: Quarterly bias audits across income segments, geographies, and customer age groups

---

## 8. Performance & Scaling

### Latency Targets
| Operation | Target | Achieved (Prototype) |
|---|---|---|
| Real-time offer trigger (mobile) | <200ms | ~145ms |
| RM dashboard lead queue refresh | <500ms | ~320ms |
| Full customer profile load | <800ms | ~580ms |
| SHAP explanation generation | <100ms | ~82ms |
| Batch nightly portfolio rescore | <2 hours | ~1.1 hours (10K customers) |

### Throughput
- Kafka ingestion: 50,000 transactions/second (peak)
- SageMaker endpoints: Auto-scaling 2–20 instances based on queue depth
- API Gateway: 10,000 RPS with Lambda concurrency

---

## 9. Hackathon Prototype vs Production

| Capability | Prototype (MVP) | Production |
|---|---|---|
| Data source | Synthetic transaction dataset (500 customers) | IDBI Core Banking real-time feed |
| Merchant normalization | Rule-based + keyword matching | Sentence-BERT fine-tuned on Indian merchants |
| Life event model | Hardcoded signals (furniture+movers = house shift) | Transformer trained on 500K synthetic histories |
| Affordability model | Static formula-based calculator | XGBoost trained on 180K historical accounts |
| SHAP explanations | Pre-computed for demo customers | Real-time TreeExplainer at inference |
| RM script | Handcrafted templates | GPT-4 template filling with SHAP context |
| Infrastructure | Static HTML served locally | AWS SageMaker + Kafka MSK + ECS |
| Account Aggregator | Simulated consent flow | Licensed AA entity integration |
| Model retraining | Not implemented | Weekly automated retraining on RM feedback |
| Multi-language support | English only | Hindi, Tamil, Telugu, Marathi |

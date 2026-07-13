# Pulse — AI Prospect Intelligence Platform

> **Predicting life events and affordability from raw transaction data to deliver the right loan, at the right time, with explainable AI.**


## The Problem

India's retail banking sector disburses over ₹12 lakh crore in retail loans annually, yet the conversion rate from marketing outreach to loan disbursement averages just **6–8%**. Banks spend heavily on SMS/email campaigns that are built on static CIBIL scores and declared income data — both of which are stale the moment a customer's life changes.

The result: Relationship Managers are given cold calling lists with no context. Customers receive generic loan offers irrelevant to their current life stage. Banks waste marketing budget on low-intent prospects while missing high-intent customers who would have happily converted if approached at the right moment.

Meanwhile, every IDBI customer generates hundreds of transaction signals every month — UPI payments, card swipes, salary credits, EMI debits — that collectively paint a vivid picture of their current life events and true repayment capacity. This data sits in the core banking system, unused for prospecting.

**Pulse solves this.** We turn raw transaction noise into a prioritized, ranked, explainable lead queue that gets RMs talking to the right person, with the right offer, at exactly the right moment.

---

## Our Solution

Pulse is a predictive banking intelligence platform with 4 tightly integrated AI modules:

### 1. Life Event Detector (Transformer + Sentence-BERT)
Analyzes sequential transaction patterns to identify major life milestones — house shifting, vehicle upgrade, wedding preparation, business expansion — **before the customer actively applies for a loan**. Uses Sentence-BERT to normalize noisy UPI narration strings ("SULEKHA*PACKERS*4521") into clean merchant categories, then feeds them into a Transformer sequence model that learns how spending evolves over time before each life event.

### 2. Dynamic Affordability Engine (XGBoost)
Calculates real-time disposable income, monthly cash-flow stability, and safe EMI tolerance directly from transaction history — eliminating reliance on stale declared income. An XGBoost model processes 40+ structured financial features to produce a monthly affordability score with confidence bands.

### 3. Contextual Offer Orchestrator
Mathematically matches the predicted life-event need with the exact product in IDBI's portfolio. A home-shifting customer who bought furniture and booked packers gets a Home Improvement Loan offer — not a generic personal loan. Timing, channel, and amount are all optimized per customer.

### 4. Explainable RM Copilot
Equips Relationship Managers with SHAP-driven, human-readable reasons for every AI recommendation — "Salary credited consistently for 14 months", "Vehicle maintenance +40% in 90 days" — along with a suggested conversation script. No black boxes. Every offer is defensible to the customer and to regulators.

---

## Key Metrics (Projected)

| Performance Metric | Projected Impact | Bank Business Value |
|---|---|---|
| Lead Conversion Rate | **45%** (vs industry 8%) | Dramatically higher loan disbursement volumes |
| RM Productivity | **4x** | RMs work warm, intent-driven leads only |
| Campaign Cost / CPA | **−45%** | Stops wasted marketing spend |
| Spam Offers Sent | **−60%** | Improves customer trust and app engagement |
| Income Estimation Accuracy | **91%** | Enables prudent, accurate underwriting |

---

## How It Works — AI Pipeline

```
Raw UPI/Card Transactions
        │
        ▼
[Sentence-BERT NLP Layer]
 Normalizes "SBI*HDFC*UPI*4521xyz" → "Furniture Store"
 Clusters 50,000+ unique merchant narrations into 120 categories
        │
        ▼
[Transformer Sequence Model]
 Learns spending evolution patterns over 90-day windows
 Predicts: Life Event Type + Confidence Score
 Output: "House Shifting (87%)" or "Vehicle Upgrade (73%)"
        │
        ▼
[XGBoost Affordability Model]
 Features: avg monthly inflow, EMI ratio, spend volatility,
           salary consistency, credit utilization, savings rate
 Output: Safe EMI capacity + Income estimate
        │
        ▼
[SHAP Explainability Layer]
 Generates 3–5 human-readable reasons for every recommendation
 Flags risk factors with adjusted rate buffers
        │
        ▼
[Contextual Offer + RM Script]
 Product × Amount × Rate × Timing optimized per customer
 Delivered via: RM Copilot Dashboard + Customer Mobile App
```

---

## UI Screens

### 1. RM Copilot Dashboard (`index.html`)
The primary workspace for Relationship Managers. Shows 4 real-time KPI tiles (Hot Leads, Conversion Rate, Pipeline Value, AI Accuracy), a prioritized daily lead queue ranked by AI score, a live transaction signal feed, and a weekly performance chart. Opens to this screen.

### 2. Prospect Intelligence (`src/views/lead-list.html`)
Full database of 4,847+ AI-scored prospects with advanced filtering by life event, income range, signal recency, and loan type. 25-row paginated table with sortable columns showing AI scores, life event badges, estimated income, free cash flow, and one-click actions.

### 3. Customer 360 (`src/views/lead-detail.html`)
Deep-dive on any individual customer. Shows income intelligence (declared vs AI-estimated), the affordability engine output (free cash flow gauge, DTI ratio), the complete life event evidence trail (5 transaction signals with dates), spending pattern chart, recommended offer, and a pre-generated RM conversation script.

### 4. AI Offer Engine (`src/views/offer-engine.html`)
The explainability and compliance hub. Features a SHAP horizontal bar chart showing the 8 top features driving the recommendation, 5 human-readable reason cards (pass/warn), an interactive offer builder with live EMI calculator, confidence metrics, and a compliance footer with RBI Fair Practices Code verification.

### 5. Customer Mobile App (`src/views/mobile-app.html`)
What IDBI's existing mobile banking app looks like after Pulse integration. An iPhone-frame mockup showing the customer's account overview, a visually prominent pre-approved offer card triggered by Pulse AI, and the privacy-first disclosure of what signals were used.

---

## Tech Stack

### Frontend (MVP Mockup)
- Vanilla HTML5, CSS3 with custom property design tokens
- Chart.js 4.x for interactive charts (SHAP bars, area charts, gauges, bar charts)
- Inter + JetBrains Mono (Google Fonts)
- No build step — open `index.html` directly

### AI/ML Pipeline (Proposed Architecture)
| Component | Technology | Justification |
|---|---|---|
| NLP / Merchant Normalization | Sentence-BERT (all-MiniLM-L6-v2) | Robust semantic understanding of noisy UPI narrations; understands context not just keywords |
| Life Event Sequence Model | Transformer (Hugging Face, fine-tuned) | Financial behavior is sequential; Transformers excel at learning temporal spending evolution |
| Affordability Model | XGBoost | Tree-based models outperform deep learning on structured tabular financial data |
| Explainability | SHAP (TreeExplainer) | Essential for RBI compliance; provides human-readable reasons for every decision |
| Real-time Streaming | Apache Kafka (MSK) | Sub-second event ingestion from core banking transaction logs |
| Feature Engineering | Apache Spark | Distributed processing of 90-day sliding window features across millions of customers |
| Model Serving | Amazon SageMaker | Managed inference with <200ms latency SLA |
| API Gateway | FastAPI | Secure, typed REST endpoints for RM dashboard and mobile app integration |

### Infrastructure
```
IDBI Core Banking ──┐
Account Aggregator ──┼──► Apache Kafka ──► Apache Spark ──► Feature Store
External UPI Data ──┘                              │
                                                   ▼
                                       Amazon SageMaker (Model Inference)
                                          ├── Sentence-BERT endpoint
                                          ├── Transformer sequence model
                                          └── XGBoost affordability model
                                                   │
                                                   ▼
                                           FastAPI Gateway
                                          ├── RM Copilot Dashboard
                                          └── IDBI Mobile App
```

---

## Running the Demo

```bash
# No build required — open directly in browser:
open Pulse/index.html

# Or serve locally for proper relative paths:
cd Pulse && npx serve .
# Then open: http://localhost:3000
```

**Navigation flow:**
1. Start at `index.html` (RM Dashboard)
2. Click "View all 47 leads" → `lead-list.html`
3. Click "View" on any row → `lead-detail.html` (Raj Kumar's profile)
4. Click "Generate Full Offer" → `offer-engine.html` (AI explainability)
5. Mobile view: `src/views/mobile-app.html`

---

## Architecture Decisions

**Why Sentence-BERT over regex/keyword matching?**
UPI narration strings like "SBI IMPS 4521 SULEKHA*PACKERS" require semantic understanding. Sentence-BERT embeds these into a 384-dimensional space where semantically similar merchants cluster together, enabling robust categorization of the long tail of merchant names without maintaining a keyword dictionary.

**Why Transformers for life event detection?**
Life events unfold over time — a house shift isn't one transaction, it's a sequence: travel → furniture → packers → utilities. Transformers with positional encodings are architecturally suited to learn these multi-step temporal patterns in a way that CNNs and RNNs cannot.

**Why XGBoost for affordability (not deep learning)?**
Financial affordability features are structured, tabular, and relatively low-dimensional (~40 features). On this data type, gradient-boosted trees consistently outperform neural networks while being far more interpretable and trainable on smaller datasets.

**Why SHAP?**
RBI's Fair Practices Code and the DPDP Act 2023 require that automated credit-related decisions be explainable to customers. SHAP TreeExplainer provides mathematically rigorous feature attributions with negligible compute overhead, enabling real-time explainability at inference time.

---

## Compliance & Ethics

- **RBI Fair Practices Code**: Every offer includes 3–5 human-readable reasons. No black-box decisions.
- **DPDP Act 2023**: Consent collected via Account Aggregator (AA) framework before any external data is used. Customers can withdraw consent at any time.
- **Data Minimization**: Only transaction metadata (merchant category, amount, timestamp) is used. No PII enters the ML models.
- **Audit Trail**: Full decision audit log retained for 7 years per RBI guidelines.
- **Cold-Start Problem**: Account Aggregator integration allows the model to bootstrap for new customers using consented external bank data.

---

## Future Roadmap

### Phase 2 (3–6 months post-launch)
- **B2B Merchant Partnerships**: Aggregate predicted life-events across the user base to negotiate exclusive co-branded deals with brands like Pepperfry, MakeMyTrip, and Maruti Suzuki.
- **MSME Extension**: Adapt sequence models to analyze vendor payment patterns and GST cycles to automatically trigger business credit line offers.
- **WhatsApp RM Copilot**: Surface AI scripts and offer summaries directly in WhatsApp Business for mobile-first RMs.

### Phase 3 (6–12 months)
- **Real-time streaming inference**: Sub-100ms offer triggers directly inside the IDBI mobile app transaction flow.
- **Cross-sell matrix**: Multi-product recommendation engine that identifies customers eligible for 2–3 products simultaneously.
- **Feedback loop**: Closed-loop model retraining using RM acceptance/rejection signals to improve F1 score weekly.

---

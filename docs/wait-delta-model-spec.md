# Park Predictor — Wait Delta Model Spec

**Status:** design / not yet implemented
**Purpose of this document:** context for AI coding tools (Claude Code, etc.) working on the ML component. Read this before generating code for anything under the prediction module.

---

## 1. Goal

Given a snapshot of a park at time `t`, predict whether the posted wait time for a
specific ride will be **higher, lower, or about the same** at time `t + h`.

This is a **supervised multi-class classification** problem. Three classes.

Not in scope for v1:
- predicting exact future wait times (regression)
- predicting ride downtime
- cross-park transfer learning
- anything involving neural networks

---

## 2. Why classification and not regression

Posted wait times are:
- rounded to the nearest 5 minutes
- published by park operations, not measured — they are a demand-management lever
  as much as an observation

Predicting an exact minute value implies a precision the source data does not have.
Three coarse buckets with a dead band absorbs the quantization noise.

---

## 3. Label definition

```
delta = wait[ride, t + h] - wait[ride, t]

label = "down"  if delta < -DEAD_BAND
        "up"    if delta >  DEAD_BAND
        "flat"  otherwise
```

**Constants:**

| Name        | v1 value | Notes |
|-------------|----------|-------|
| `DEAD_BAND` | 5 min    | Matches source quantization. Tune later against class balance. |
| `h`         | 30 min   | Train a separate model per horizon. v1 ships h=30 only; h=60 later. |

**Exclusion rules — drop the row entirely if:**
- the ride is closed/down at `t` OR at `t + h`
- `t + h` falls after park close
- there is a gap > 15 min in the source series between `t` and `t + h`
- `t` is within 30 min of park open (opening-surge dynamics are a different regime)

Do not impute missing waits. Drop.

---

## 4. Feature specification

One row = one `(ride_id, timestamp)` pair. Target sampling interval: 5 minutes.

### 4.1 Ride trajectory
| Feature | Type | Notes |
|---|---|---|
| `wait_now` | int | posted wait at `t` |
| `wait_lag_15`, `wait_lag_30`, `wait_lag_60` | int | posted wait at t−15/30/60 min |
| `delta_15`, `delta_30` | int | `wait_now` minus the corresponding lag |
| `slope_60` | float | least-squares slope over trailing 60 min |
| `wait_vs_ride_median` | float | `wait_now` minus this ride's historical median for (time-of-day bucket × day-of-week) |
| `is_down` | bool | ride currently reported closed |
| `mins_since_reopen` | int | null if no reopen today; **high-signal, do not omit** |
| `downtime_mins_today` | int | cumulative |

### 4.2 Park context
| Feature | Type | Notes |
|---|---|---|
| `park_mean_normalized_wait` | float | mean across open rides of (wait / that ride's historical median). Live crowd proxy. |
| `park_mean_slope_60` | float | slope of the above |
| `pct_rides_down` | float | |
| `neighbor_wait_1..3` | int | 3 nearest rides by walking distance, fixed per ride |
| `neighbor_delta_30_mean` | float | |

### 4.3 Calendar / clock
| Feature | Type | Notes |
|---|---|---|
| `mins_since_open`, `mins_to_close` | int | |
| `tod_sin`, `tod_cos` | float | cyclical encoding of minute-of-day |
| `doy_sin`, `doy_cos` | float | cyclical encoding of day-of-year |
| `day_of_week` | categorical | |
| `days_to_nearest_holiday` | int | signed |
| `is_school_break` | bool | regional; source TBD |
| `has_evening_event` | bool | hard-ticket party / after-hours |
| `is_extra_magic_hours` | bool | or park equivalent |

### 4.4 Exogenous
| Feature | Type | Notes |
|---|---|---|
| `temp_f`, `precip_mm_now` | float | |
| `precip_prob_next_60` | float | forecast, not actual — must be the forecast *as known at `t`* |
| `mins_to_next_parade`, `mins_to_next_fireworks` | int | scheduled entertainment reshapes queues predictably |

### 4.5 Identity
| Feature | Type | Notes |
|---|---|---|
| `ride_id` | categorical | pass as native categorical to LightGBM, do not one-hot |
| `park_id` | categorical | |

**Leakage rule:** every feature must be computable from information available at
time `t` only. `precip_prob_next_60` is legal because it's a forecast issued before
`t`. Actual observed rainfall between `t` and `t+h` is **not** legal.

---

## 5. Output contract

```json
{
  "ride_id": "wdw_mk_space_mountain",
  "predicted_at": "2026-07-21T14:15:00Z",
  "horizon_mins": 30,
  "probabilities": { "down": 0.61, "flat": 0.29, "up": 0.10 },
  "predicted_class": "down",
  "model_version": "wait-delta-h30-v1.2.0"
}
```

Probabilities must sum to 1.0. Consumers should be able to threshold on confidence
and suppress low-confidence predictions in the UI — so **calibration is a first-class
requirement**, not raw accuracy.

---

## 6. Data volume targets

The meaningful unit is **ride-days**, not rows. 5-minute samples within a ride-day are
heavily autocorrelated and are not independent observations.

| Tier | Coverage | Ride-days | Approx rows | Expected capability |
|---|---|---|---|---|
| Toy | 1 month | ~1,500 | ~200k | Learns intraday shape only |
| **Viable (v1 target)** | 6 months | ~9,000 | ~1.3M | Weekday/weekend, weather, crowd level |
| Confident | 2+ years | ~35,000 | ~5M | Holiday and seasonal generalization |

Annual seasonality is the binding constraint. In a single year each holiday period
occurs exactly once and is indistinguishable from an idiosyncratic week.

**Implication for sourcing:** the TouringPlans historical datasets are required for
cold start. The Queue-Times API only accrues data forward from first poll. Backfill
first, then rely on live polling to extend.

---

## 7. Model

**v1: LightGBM multiclass classifier.** Do not propose neural / sequence models
(LSTM, Transformer, TFT) for v1 — for tabular data at this volume they require
substantially more data and tuning to match gradient-boosted trees.

- objective: `multiclass`, `num_class=3`
- metric: `multi_logloss`
- start with library defaults; do not tune hyperparameters until baselines are beaten
- pass `ride_id` / `park_id` as pandas `category` dtype
- early stopping on the validation fold

---

## 8. Baselines (mandatory before any model is considered working)

Both must be implemented and scored on the identical test split:

1. **Persistence** — always predict `flat`.
2. **Climatology** — predict the historical modal class for
   (ride × day-of-week × time-of-day bucket).

A model that does not beat both on log loss is not shippable. Record all three
scores in every experiment.

---

## 9. Evaluation protocol

**Splitting:** temporal, by whole operating day.
- train: earliest 70% of days
- validation: next 15% of days
- test: most recent 15% of days

Never shuffle rows across the split boundary. Adjacent 5-minute samples from the
same afternoon on opposite sides of a split is the primary leakage risk in this
project and will produce inflated scores that collapse in production.

**Metrics to report, in priority order:**
1. multiclass log loss (primary)
2. reliability diagram / calibration curve per class
3. per-class precision & recall
4. accuracy (reported, not optimized)
5. lift over each baseline

Also break down performance by: time of day, day of week, and crowd level tercile.
Aggregate metrics hide the failure modes that matter.

---

## 10. Suggested module layout

```
prediction/
  data/
    loaders.py        # pull from the existing processor DAG outputs
    labeling.py       # delta -> class, exclusion rules
    splits.py         # day-wise temporal split
  features/
    trajectory.py     # 4.1
    park_context.py   # 4.2
    calendar.py       # 4.3
    exogenous.py      # 4.4
    registry.py       # decorator-based feature registration, mirroring the
                      # existing processor registration pattern
  models/
    baselines.py      # persistence, climatology
    lightgbm_model.py
  eval/
    metrics.py
    calibration.py
    reports.py
  serving/
    predict.py        # inference path — must share feature code with training
```

**Hard requirement:** training and serving must call the *same* feature-computation
functions. Duplicated feature logic between the two paths (training/serving skew) is
the second most common way this class of project silently fails.

---

## 11. Build order

1. Ingestion + backfill running and verified. This is the long pole — everything
   downstream is fast.
2. Labeling module + exclusion rules, with tests.
3. Day-wise splitter, with a test asserting no date appears in two splits.
4. Both baselines. **Record their scores. This is the bar.**
5. ~10 features from §4.1 and §4.3 only. Train LightGBM defaults. Compare.
6. Expand features incrementally, re-scoring against baselines each time.
7. Calibration check and reliability diagram.
8. Serving path, sharing feature code with training.

Do not skip step 4.

---

## 12. Open questions

- Source for regional school-break calendars
- Whether ride-specific models beat one pooled model with `ride_id` as a feature
  (start pooled; it shares information across rides and handles low-volume rides better)
- Whether `DEAD_BAND` of 5 produces workable class balance, or needs widening
- Retraining cadence once live

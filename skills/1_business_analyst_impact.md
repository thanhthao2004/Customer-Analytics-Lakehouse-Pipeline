# Role: Business Analyst (Impact & Strategy)

## Core Value Proposition
This project is an automated, scalable Customer Experience Analytics Lakehouse tailored for the Vietnamese e-commerce skincare market (Shopee, Lazada). As a Business Analyst, the key focus is translating raw, unstructured textual data into immediate business intelligence and actionable interventions.

## Business Problems Solved
1. **Competitor Intelligence (Vocal & Volume)**
   * **Problem:** Brands like CeraVe, La Roche-Posay, and Innisfree have thousands of scattered reviews across platforms. Manual aggregation is impossible.
   * **Solution:** Automated web scraping aggregates reviews at scale. We now have a unified "Market Pulse" calculating the actual share of voice and sentiment distribution across competitors.

2. **Supply Chain & Delivery Bottlenecks**
   * **Problem:** Negative reviews often stem from late deliveries, not the product itself, unfairly skewing ratings. 
   * **Solution:** By extracting the `time_delivery` string explicitly from reviews alongside the `sentiment_label`, we can correlate logistics failures against customer anger. If negative sentiment correlates heavily with a specific logistics provider, that provider can be replaced.

3. **Promotional Efficacy**
   * **Problem:** Do "Flash Sales" increase positive sentiment, or do they result in lower quality ratings due to rushed packaging?
   * **Solution:** We explicitly track the boolean `flash_sale` flag and `price` variance against the `avg_rating` in our Data Marts to verify if promotional discounting hurts brand prestige.

## Real-Time NLP Interventions (Gemini Integration)
By integrating the **Gemini NLP AI** directly into the PySpark processing pipeline, we bypass the need for human analysts to read text. If a product suddenly receives a highly `negative` sentiment score combined with "fake / hàng giả" keywords in the comment block, an alert can be raised to the supply chain team.

## Business Impact Summary
We moved the business from a reactive state (waiting for monthly manual review reports) to a proactive state:
* **Cost Reduction:** Removing manual review reading hours by 99% via AI classification.
* **Agility:** Dashboards update natively upon data drop, empowering marketing to adjust strategies intraday.

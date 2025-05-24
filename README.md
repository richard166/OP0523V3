# Taiwan Company Contacts Crawler

This project collects company contact information from the OpenData VIP API and merges the results into a single CSV file.

## Sources
- **OpenData VIP 公司 API**

## 需要
* Python 3.10+
* `pip install -r requirements.txt`

## 執行
Run the crawler manually:

```bash
python -m src.crawler
```

The result will be saved under `csv/output/` with the current date.

## GitHub Actions
A workflow runs every Monday at 01:00 UTC to execute the crawler automatically.

## Fetching by Business Number

If you only need data for specific companies, the script
`company_api_crawler/fetch_by_id.py` can query the OpenData VIP API using a list
of business numbers:

```bash
python company_api_crawler/fetch_by_id.py 13133518 16669823
```

You may also pass a text file containing one ID per line:

```bash
python company_api_crawler/fetch_by_id.py -f ids.txt
```

The resulting CSV will be stored under `company_api_crawler/data/output/`.

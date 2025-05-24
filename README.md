# Taiwan Company Contacts Crawler

This project collects company contact information from multiple Taiwan-based sources and merges them into a single CSV file.

## Sources
- **OpenData VIP 公司資料**

## 需要
* Python 3.10+
* `pip install -r requirements.txt`

## 執行
Run the crawler manually:

```bash
python -m src.crawler
```

To view detailed logging in real time, pass the `-v` flag. You can also use
`--log FILE` to save the log messages to a file.

```bash
python -m src.crawler -v --log crawler.log
```

The result will be saved under `csv/output/` with the current date.

## GitHub Actions
A workflow runs every Monday at 01:00 UTC to execute the crawler automatically.

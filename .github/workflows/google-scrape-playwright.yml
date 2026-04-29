name: Daily Google Scraper (Playwright)

on:
  schedule:
    # KST 08:37 = UTC 23:37 (전일)
    - cron: '37 23 * * *'
  workflow_dispatch:
    inputs:
      extra_keyword:
        description: 'Extra keyword to scrape (optional)'
        required: false
        default: ''

jobs:
  scrape:
    runs-on: ubuntu-latest
    timeout-minutes: 30

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          cd backend/backend
          pip install -r requirements.txt
          pip install playwright
          playwright install --with-deps chromium

      - name: Random initial sleep (0~3600s)
        run: |
          SLEEP=$((RANDOM % 3600))
          echo "Sleeping ${SLEEP} seconds before scrape..."
          sleep $SLEEP

      - name: Run scraper
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
          APP_ENV: prod
          ACCOUNT_NAME: mannacom
          EXTRA_KEYWORD: ${{ github.event.inputs.extra_keyword }}
        run: |
          cd backend/backend
          python google_scraper_playwright.py

      - name: Update views
        if: success()
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_ROLE_KEY: ${{ secrets.SUPABASE_SERVICE_ROLE_KEY }}
          APP_ENV: prod
        run: |
          cd backend/backend
          python view_updater.py || true

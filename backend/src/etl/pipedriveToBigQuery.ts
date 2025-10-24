import axios from 'axios';
import { BigQuery } from '@google-cloud/bigquery';
import dotenv from 'dotenv';
dotenv.config();

// Força uso das credenciais padrão no Windows
process.env.GOOGLE_APPLICATION_CREDENTIALS =
  process.env.GOOGLE_APPLICATION_CREDENTIALS ||
  `${process.env.APPDATA}\\gcloud\\application_default_credentials.json`;

const PIPEDRIVE_API_TOKEN = process.env.PIPEDRIVE_API_TOKEN!;
const BIGQUERY_PROJECT = process.env.BQ_PROJECT!;
const BIGQUERY_DATASET = process.env.BQ_DATASET!;

const bigquery = new BigQuery({ projectId: BIGQUERY_PROJECT });

async function fetchDeals(start = 0, limit = 500) {
  const url = `https://api.pipedrive.com/v1/deals?start=${start}&limit=${limit}&api_token=${PIPEDRIVE_API_TOKEN}`;
  const resp = await axios.get(url);
  if (!resp.data.success) {
    throw new Error(`Pipedrive API error: ${JSON.stringify(resp.data)}`);
  }
  return resp.data.data;
}

async function insertDeals(rows: any[]) {
  const dataset = bigquery.dataset(BIGQUERY_DATASET);
  const table = dataset.table('deals_raw');

  const cleaned = rows.map((deal) => ({
    id: deal.id,
    title: deal.title || '',
    status: deal.status || '',
    value: deal.value || 0,
    currency: deal.currency || '',
    add_time: deal.add_time ? new Date(deal.add_time) : null,
    update_time: deal.update_time ? new Date(deal.update_time) : null,
    user_id: deal.user_id?.id || null,
    org_id: deal.org_id || null,
    stage_id: deal.stage_id || null,
    custom_fields: JSON.stringify(deal), // opcional: debug ou log
  }));

  try {
    await table.insert(cleaned);
    console.log(`✅ Inserted ${cleaned.length} deals`);
  } catch (err: any) {
    console.error('❌ BigQuery insert error:', err.name);
    if (err.errors) {
      console.error('🔍 Sample error row:', JSON.stringify(err.errors[0], null, 2));
    }
  }
}

async function runETL() {
  try {
    let start = 0;
    const limit = 500;
    while (true) {
      const deals = await fetchDeals(start, limit);
      if (!deals || deals.length === 0) break;
      await insertDeals(deals);
      start += deals.length;
      if (deals.length < limit) break;
    }
    console.log('✅ ETL completed at', new Date().toISOString());
  } catch (err) {
    console.error('ETL error:', err);
    process.exit(1);
  }
}

runETL();
